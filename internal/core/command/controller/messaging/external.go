//
// Copyright (C) 2022-2023 IOTech Ltd
// Copyright (C) 2023 Intel Inc.
//
// SPDX-License-Identifier: Apache-2.0

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	bootstrapContainer "github.com/edgexfoundry/go-mod-bootstrap/v3/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v3/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/dtos/requests"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/models"
	"github.com/google/uuid"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"

	"github.com/edgexfoundry/edgex-go/internal/core/command/container"
)

const (
	MetadataRequestTopic        = "MetadataRequestTopic"
	MetadataResponseTopicPrefix = "MetadataResponseTopicPrefix"
)

func OnConnectHandler(requestTimeout time.Duration, dic *di.Container) mqtt.OnConnectHandler {
	return func(client mqtt.Client) {
		lc := bootstrapContainer.LoggingClientFrom(dic.Get)
		config := container.ConfigurationFrom(dic.Get)
		externalTopics := config.ExternalMQTT.Topics
		qos := config.ExternalMQTT.QoS

		requestQueryTopic := externalTopics[common.CommandQueryRequestTopicKey]
		if token := client.Subscribe(requestQueryTopic, qos, commandQueryHandler(dic)); token.Wait() && token.Error() != nil {
			lc.Errorf("could not subscribe to topic '%s': %s", requestQueryTopic, token.Error().Error())
		} else {
			lc.Debugf("Subscribed to topic '%s' on external MQTT broker", requestQueryTopic)
		}

		requestCommandTopic := externalTopics[common.CommandRequestTopicKey]
		if token := client.Subscribe(requestCommandTopic, qos, commandRequestHandler(requestTimeout, dic)); token.Wait() && token.Error() != nil {
			lc.Errorf("could not subscribe to topic '%s': %s", requestCommandTopic, token.Error().Error())
		} else {
			lc.Debugf("Subscribed to topic '%s' on external MQTT broker", requestCommandTopic)
		}

		requestMetadataTopic := externalTopics[MetadataRequestTopic]
		if token := client.Subscribe(requestMetadataTopic, qos, commandMetadataHandler(requestTimeout, dic)); token.Wait() && token.Error() != nil {
			lc.Errorf("could not subscribe to topic '%s': %s", requestMetadataTopic, token.Error().Error())
		} else {
			lc.Debugf("Subscribed to topic '%s' on external MQTT broker", requestMetadataTopic)
		}

	}
}

func commandQueryHandler(dic *di.Container) mqtt.MessageHandler {
	return func(client mqtt.Client, message mqtt.Message) {
		lc := bootstrapContainer.LoggingClientFrom(dic.Get)
		lc.Debugf("Received command query request from external message broker on topic '%s' with %d bytes", message.Topic(), len(message.Payload()))

		requestEnvelope, err := types.NewMessageEnvelopeFromJSON(message.Payload())
		if err != nil {
			lc.Errorf("Failed to decode request MessageEnvelope: %s", err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}

		externalMQTTInfo := container.ConfigurationFrom(dic.Get).ExternalMQTT
		responseTopic := externalMQTTInfo.Topics[common.CommandQueryResponseTopicKey]
		if responseTopic == "" {
			lc.Error("QueryResponseTopic not provided in External.Topics")
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}

		// example topic scheme: edgex/commandquery/request/<device-name>
		// deviceName is expected to be at last topic level.
		topicLevels := strings.Split(message.Topic(), "/")
		deviceName, err := url.PathUnescape(topicLevels[len(topicLevels)-1])
		if err != nil {
			lc.Errorf("Failed to unescape device name '%s': %s", deviceName, err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		if strings.EqualFold(deviceName, common.All) {
			deviceName = common.All
		}

		responseEnvelope, err := getCommandQueryResponseEnvelope(requestEnvelope, deviceName, dic)
		if err != nil {
			responseEnvelope = types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, err.Error())
		}

		qos := externalMQTTInfo.QoS
		retain := externalMQTTInfo.Retain
		responseEnvelope.ReceivedTopic = responseTopic
		publishMessage(client, responseTopic, qos, retain, responseEnvelope, lc)
	}
}

func commandRequestHandler(requestTimeout time.Duration, dic *di.Container) mqtt.MessageHandler {
	return func(client mqtt.Client, message mqtt.Message) {
		lc := bootstrapContainer.LoggingClientFrom(dic.Get)
		config := container.ConfigurationFrom(dic.Get)
		lc.Debugf("Received command request from external message broker on topic '%s' with %d bytes", message.Topic(), len(message.Payload()))

		externalMQTTInfo := container.ConfigurationFrom(dic.Get).ExternalMQTT
		qos := externalMQTTInfo.QoS
		retain := externalMQTTInfo.Retain

		requestEnvelope, err := types.NewMessageEnvelopeFromJSON(message.Payload())
		if err != nil {
			lc.Errorf("Failed to decode request MessageEnvelope: %s", err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}

		topicLevels := strings.Split(message.Topic(), "/")
		length := len(topicLevels)
		if length < 3 {
			lc.Error("Failed to parse and construct response topic scheme, expected request topic scheme: '#/<device-name>/<command-name>/<method>")
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}

		// expected external command request/response topic scheme: #/<device-name>/<command-name>/<method>
		deviceName, err := url.PathUnescape(topicLevels[length-3])
		if err != nil {
			lc.Errorf("Failed to unescape device name from '%s': %s", topicLevels[length-3], err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		commandName, err := url.PathUnescape(topicLevels[length-2])
		if err != nil {
			lc.Errorf("Failed to unescape command name from '%s': %s", topicLevels[length-2], err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		method := topicLevels[length-1]
		if !strings.EqualFold(method, "get") && !strings.EqualFold(method, "set") {
			lc.Errorf("Unknown request method: %s, only 'get' or 'set' is allowed", method)
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}

		externalResponseTopic := common.BuildTopic(externalMQTTInfo.Topics[common.CommandResponseTopicPrefixKey], deviceName, commandName, method)

		internalBaseTopic := config.MessageBus.GetBaseTopicPrefix()
		topicPrefix := common.BuildTopic(internalBaseTopic, common.CoreCommandDeviceRequestPublishTopic)

		deviceServiceName, err := retrieveServiceNameByDevice(deviceName, dic)
		if err != nil {
			responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, err.Error())
			publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
			return
		}

		err = validateGetCommandQueryParameters(requestEnvelope.QueryParams)
		if err != nil {
			responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, err.Error())
			publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
			return
		}

		deviceRequestTopic := common.NewPathBuilder().EnableNameFieldEscape(config.Service.EnableNameFieldEscape).
			SetPath(topicPrefix).SetNameFieldPath(deviceServiceName).SetNameFieldPath(deviceName).SetNameFieldPath(commandName).SetPath(method).BuildPath()
		deviceResponseTopicPrefix := common.NewPathBuilder().EnableNameFieldEscape(config.Service.EnableNameFieldEscape).
			SetPath(internalBaseTopic).SetPath(common.ResponseTopic).SetNameFieldPath(deviceServiceName).BuildPath()

		lc.Debugf("Sending Command request to internal MessageBus. Topic: %s, Request-id: %s Correlation-id: %s", deviceRequestTopic, requestEnvelope.RequestID, requestEnvelope.CorrelationID)
		lc.Debugf("Expecting response on topic: %s/%s", deviceResponseTopicPrefix, requestEnvelope.RequestID)

		internalMessageBus := bootstrapContainer.MessagingClientFrom(dic.Get)

		// Request waits for the response and returns it.
		response, err := internalMessageBus.Request(requestEnvelope, deviceRequestTopic, deviceResponseTopicPrefix, requestTimeout)
		if err != nil {
			errorMessage := fmt.Sprintf("Failed to send DeviceCommand request with internal MessageBus: %v", err)
			responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
			publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
			return
		}

		lc.Debugf("Command response received from internal MessageBus. Topic: %s, Request-id: %s Correlation-id: %s", response.ReceivedTopic, response.RequestID, response.CorrelationID)

		response.ReceivedTopic = externalResponseTopic
		publishMessage(client, externalResponseTopic, qos, retain, *response, lc)
	}
}

// commandMetadataHandler从外部消息代理接收处理元数据请求
// Topic格式为：<edge-box-name>/edgex/metadata/request/<object-name>/<method>
// object-name包含两种类型：device和profile，根据不同类型处理不同的元数据
// method包含：add/update/del，根据不同的method进行元数据的不同操作
func commandMetadataHandler(requestTimeout time.Duration, dic *di.Container) mqtt.MessageHandler {
	return func(client mqtt.Client, message mqtt.Message) {
		lc := bootstrapContainer.LoggingClientFrom(dic.Get)
		lc.Debugf("Received command request from external message broker on topic '%s' with %d bytes", message.Topic(), len(message.Payload()))

		externalMQTTInfo := container.ConfigurationFrom(dic.Get).ExternalMQTT
		qos := externalMQTTInfo.QoS
		retain := externalMQTTInfo.Retain

		requestEnvelope, err := types.NewMessageEnvelopeFromJSON(message.Payload())
		if err != nil {
			lc.Errorf("Failed to decode request MessageEnvelope: %s", err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		topicLevels := strings.Split(message.Topic(), "/")
		length := len(topicLevels)
		if length < 3 {
			lc.Error("Failed to parse and construct response topic scheme, expected request topic scheme: '#/<device-name>/<command-name>/<method>")
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		objectName := topicLevels[length-2]
		if err != nil {
			lc.Errorf("Failed to unescape object name '%s': %s", objectName, err.Error())
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		if !strings.EqualFold(objectName, "device") && !strings.EqualFold(objectName, "profile") {
			lc.Errorf("Unknown request commandName: %s, only 'get' or 'set' is allowed", objectName)
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		method := topicLevels[length-1]
		if !strings.EqualFold(method, "add") && !strings.EqualFold(method, "update") && !strings.EqualFold(method, "del") {
			lc.Errorf("Unknown request method: %s, only 'add' or 'del' is allowed", method)
			lc.Warn("Not publishing error message back due to insufficient information on response topic")
			return
		}
		externalResponseTopic := common.BuildTopic(externalMQTTInfo.Topics[MetadataResponseTopicPrefix], objectName, method)

		// 管理设备信息
		if strings.EqualFold(objectName, "device") {
			if strings.EqualFold(method, "add") {
				var addDevicesReq []requests.AddDeviceRequest
				var device dtos.Device
				parseDeviceErr := json.Unmarshal(requestEnvelope.Payload, &device)
				if parseDeviceErr != nil {
					lc.Errorf("Failed to parse device json : %s", parseDeviceErr.Error())
				}

				device.AdminState = models.Unlocked
				device.OperatingState = models.Up
				req := requests.NewAddDeviceRequest(device)
				addDevicesReq = append(addDevicesReq, req)

				dc := bootstrapContainer.DeviceClientFrom(dic.Get)
				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString()) //nolint: staticcheck
				responses, edgexErr := dc.Add(ctx, addDevicesReq)
				if edgexErr != nil {
					lc.Errorf("Failed to request add device : %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to add device : %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				response := responses[0]
				if response.StatusCode == http.StatusCreated {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Id), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to add device : %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return
			} else if strings.EqualFold(method, "update") {
				var updateDevicesReq []requests.UpdateDeviceRequest
				var device dtos.UpdateDevice
				parseDeviceErr := json.Unmarshal(requestEnvelope.Payload, &device)
				if parseDeviceErr != nil {
					lc.Errorf("Failed to parse updateDevice json : %s", parseDeviceErr.Error())
				}
				req := requests.NewUpdateDeviceRequest(device)
				updateDevicesReq = append(updateDevicesReq, req)

				dc := bootstrapContainer.DeviceClientFrom(dic.Get)
				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString()) //nolint: staticcheck
				responses, edgexErr := dc.Update(ctx, updateDevicesReq)
				if edgexErr != nil {
					lc.Errorf("Failed to request update device : %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to update device : %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				response := responses[0]
				if response.StatusCode == http.StatusOK {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Message), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to update device : %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return
			} else if strings.EqualFold(method, "del") {
				deviceName := string(requestEnvelope.Payload)
				if len(deviceName) == 0 {
					lc.Errorf("设备名称不能为空")
					errorMessage := fmt.Sprintf("设备名称不能为空")
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				}
				dc := bootstrapContainer.DeviceClientFrom(dic.Get)
				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString()) //nolint: staticcheck
				response, edgexErr := dc.DeleteDeviceByName(ctx, deviceName)
				if edgexErr != nil {
					lc.Errorf("Failed to request delete device : %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to delete device : %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				if response.StatusCode == http.StatusOK {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Message), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to delete device : %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return
			}
		} else if strings.EqualFold(objectName, "profile") {
			var deviceProfilesReq []requests.DeviceProfileRequest
			// 管理物模型信息
			if strings.EqualFold(method, "add") {
				var deviceProfile dtos.DeviceProfile
				parseDeviceProfileErr := json.Unmarshal(requestEnvelope.Payload, &deviceProfile)
				if parseDeviceProfileErr != nil {
					lc.Errorf("Failed to parse device profile json : %s", parseDeviceProfileErr.Error())
				}
				dpReq := requests.NewDeviceProfileRequest(deviceProfile)
				deviceProfilesReq = append(deviceProfilesReq, dpReq)

				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString())
				dpc := bootstrapContainer.DeviceProfileClientFrom(dic.Get)
				responses, edgexErr := dpc.Add(ctx, deviceProfilesReq)
				if edgexErr != nil {
					lc.Errorf("Failed to request add device profile: %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to add device profile: %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				response := responses[0]
				if response.StatusCode == http.StatusCreated {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Message), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to add device profile : %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return

			} else if strings.EqualFold(method, "update") {
				var deviceProfile dtos.DeviceProfile
				parseDeviceProfileErr := json.Unmarshal(requestEnvelope.Payload, &deviceProfile)
				if parseDeviceProfileErr != nil {
					lc.Errorf("Failed to parse device profile json : %s", parseDeviceProfileErr.Error())
				}
				dpReq := requests.NewDeviceProfileRequest(deviceProfile)
				deviceProfilesReq = append(deviceProfilesReq, dpReq)

				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString())
				dpc := bootstrapContainer.DeviceProfileClientFrom(dic.Get)
				responses, edgexErr := dpc.Update(ctx, deviceProfilesReq)
				if edgexErr != nil {
					lc.Errorf("Failed to request update device profile: %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to add device profile: %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				response := responses[0]
				if response.StatusCode == http.StatusOK {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Message), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to update device profile : %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return
			} else if strings.EqualFold(method, "del") {
				profileName := string(requestEnvelope.Payload)
				if len(profileName) == 0 {
					lc.Errorf("物模型名称不能为空")
					errorMessage := fmt.Sprintf("物模型名称不能为空")
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				}
				dc := bootstrapContainer.DeviceProfileClientFrom(dic.Get)
				ctx := context.WithValue(context.Background(), common.CorrelationHeader, uuid.NewString()) //nolint: staticcheck
				response, edgexErr := dc.DeleteByName(ctx, profileName)
				if edgexErr != nil {
					lc.Errorf("Failed to request delete device profile: %s", edgexErr.Error())
					errorMessage := fmt.Sprintf("Failed to delete device profile: %v", edgexErr)
					responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, errorMessage)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				if response.StatusCode == http.StatusOK {
					responseEnvelope, _ := types.NewMessageEnvelopeForResponse([]byte(response.Message), requestEnvelope.RequestID, requestEnvelope.CorrelationID, common.ContentTypeText)
					publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
					return
				}
				lc.Errorf("Failed to delete device profile: %s", response.Message)
				responseEnvelope := types.NewMessageEnvelopeWithError(requestEnvelope.RequestID, response.Message)
				publishMessage(client, externalResponseTopic, qos, retain, responseEnvelope, lc)
				return
			}
		}
		lc.Errorf("to be continue ...")

	}
}

func publishMessage(client mqtt.Client, responseTopic string, qos byte, retain bool, message types.MessageEnvelope, lc logger.LoggingClient) {
	if message.ErrorCode == 1 {
		lc.Error(string(message.Payload))
	}

	envelopeBytes, _ := json.Marshal(&message)

	if token := client.Publish(responseTopic, qos, retain, envelopeBytes); token.Wait() && token.Error() != nil {
		lc.Errorf("Could not publish to external message broker on topic '%s': %s", responseTopic, token.Error())
	} else {
		lc.Debugf("Published response message to external message broker on topic '%s' with %d bytes", responseTopic, len(envelopeBytes))
	}
}
