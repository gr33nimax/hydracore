package libbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const hydraCoreMaxRemoteConfigBytes = 12 * 1024 * 1024

type hydraCoreValidationDiagnostic struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Path     string `json:"path"`
	Message  string `json:"message"`
}

type hydraCoreValidationResult struct {
	SchemaVersion int                             `json:"schema_version"`
	Profile       string                          `json:"profile"`
	Valid         bool                            `json:"valid"`
	Diagnostics   []hydraCoreValidationDiagnostic `json:"diagnostics"`
}

type hydraCorePolicyError struct {
	code    string
	path    string
	message string
}

func (e *hydraCorePolicyError) Error() string { return e.message }

func HydraCoreValidateConfig(configContent string, profile string) string {
	result := hydraCoreValidationResult{
		SchemaVersion: 1,
		Profile:       profile,
		Diagnostics:   []hydraCoreValidationDiagnostic{},
	}
	var err error
	switch profile {
	case "local":
		err = CheckConfig(configContent)
		if err != nil {
			result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
				Severity: "error",
				Code:     "native_config_invalid",
				Path:     "$",
				Message:  "configuration failed native validation",
			})
		}
	case "remote_v2":
		err = validateHydraRemoteConfigV2(configContent)
		if err == nil {
			err = CheckConfig(configContent)
			if err != nil {
				result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
					Severity: "error",
					Code:     "native_config_invalid",
					Path:     "$",
					Message:  "remote configuration failed native validation",
				})
			}
		} else if policyErr, loaded := err.(*hydraCorePolicyError); loaded {
			result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
				Severity: "error",
				Code:     policyErr.code,
				Path:     policyErr.path,
				Message:  policyErr.message,
			})
		}
	default:
		err = &hydraCorePolicyError{code: "unknown_profile", path: "$.profile", message: "unknown validation profile"}
		result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
			Severity: "error",
			Code:     "unknown_profile",
			Path:     "$.profile",
			Message:  "unknown validation profile",
		})
	}
	result.Valid = err == nil
	content, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return `{"schema_version":1,"profile":"","valid":false,"diagnostics":[{"severity":"error","code":"internal_error","path":"$","message":"validation result could not be encoded"}]}`
	}
	return string(content)
}

func validateHydraRemoteConfigV2(configContent string) error {
	content := []byte(configContent)
	if len(content) > hydraCoreMaxRemoteConfigBytes {
		return &hydraCorePolicyError{code: "document_too_large", path: "$", message: "remote configuration exceeds the size limit"}
	}
	if err := rejectDuplicateJSONKeys(content); err != nil {
		return err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(content, &document); err != nil {
		return &hydraCorePolicyError{code: "invalid_json", path: "$", message: "remote configuration is not valid JSON"}
	}
	allowedTopLevel := map[string]bool{"$schema": true, "inbounds": true, "outbounds": true, "endpoints": true}
	for field := range document {
		if !allowedTopLevel[field] {
			return &hydraCorePolicyError{code: "unsafe_top_level_field", path: "$." + field, message: "top-level field is not allowed by remote policy v2"}
		}
	}

	inbounds, err := decodeRemoteTypedObjects(document["inbounds"], "$.inbounds")
	if err != nil {
		return err
	}
	outbounds, err := decodeRemoteTypedObjects(document["outbounds"], "$.outbounds")
	if err != nil {
		return err
	}
	endpoints, err := decodeRemoteTypedObjects(document["endpoints"], "$.endpoints")
	if err != nil {
		return err
	}

	safeInbounds := map[string]bool{}
	if hydraCoreCallEnabled {
		safeInbounds["call"] = true
	}
	safeOutbounds := map[string]bool{
		"socks": true, "http": true, "vmess": true, "trojan": true,
		"naive": true, "shadowtls": true, "vless": true, "mieru": true,
		"anytls": true, "trusttunnel": true, "hysteria": true,
		"hysteria2": true, "tuic": true, "sudoku": true, "snell": true,
	}
	if hydraCoreCallEnabled {
		safeOutbounds["call"] = true
	}

	for index, object := range inbounds {
		if !safeInbounds[object.Type] {
			return &hydraCorePolicyError{code: "unsafe_inbound_type", path: fmt.Sprintf("$.inbounds[%d].type", index), message: "inbound type is not allowed by remote policy v2"}
		}
	}
	for index, object := range outbounds {
		if !safeOutbounds[object.Type] {
			return &hydraCorePolicyError{code: "unsafe_outbound_type", path: fmt.Sprintf("$.outbounds[%d].type", index), message: "outbound type is not allowed by remote policy v2"}
		}
		if object.Type != "call" {
			if err := rejectLocalAuthorityFields(object.Raw, fmt.Sprintf("$.outbounds[%d]", index)); err != nil {
				return err
			}
		}
	}
	for index, object := range endpoints {
		if object.Type != "wireguard" {
			return &hydraCorePolicyError{code: "unsafe_endpoint_type", path: fmt.Sprintf("$.endpoints[%d].type", index), message: "endpoint type is not allowed by remote policy v2"}
		}
		if err := rejectLocalAuthorityFields(object.Raw, fmt.Sprintf("$.endpoints[%d]", index)); err != nil {
			return err
		}
	}
	return validateRemoteReferenceGraph(inbounds, outbounds, endpoints)
}

type hydraCoreRemoteObject struct {
	Type string
	Tag  string
	Raw  map[string]any
}

func decodeRemoteTypedObjects(content json.RawMessage, path string) ([]hydraCoreRemoteObject, error) {
	if len(content) == 0 || bytes.Equal(bytes.TrimSpace(content), []byte("null")) {
		return []hydraCoreRemoteObject{}, nil
	}
	var rawObjects []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if err := decoder.Decode(&rawObjects); err != nil {
		return nil, &hydraCorePolicyError{code: "invalid_object_list", path: path, message: "remote object section must be an array"}
	}
	objects := make([]hydraCoreRemoteObject, 0, len(rawObjects))
	for index, rawObject := range rawObjects {
		typeName, typeLoaded := rawObject["type"].(string)
		tag, tagLoaded := rawObject["tag"].(string)
		if !typeLoaded || typeName == "" {
			return nil, &hydraCorePolicyError{code: "missing_type", path: fmt.Sprintf("%s[%d].type", path, index), message: "remote object requires a type"}
		}
		if !tagLoaded || tag == "" {
			return nil, &hydraCorePolicyError{code: "missing_tag", path: fmt.Sprintf("%s[%d].tag", path, index), message: "remote object requires an explicit tag"}
		}
		if strings.HasPrefix(tag, "__hydra.") {
			return nil, &hydraCorePolicyError{code: "reserved_tag", path: fmt.Sprintf("%s[%d].tag", path, index), message: "remote object uses a Hydra-reserved tag"}
		}
		objects = append(objects, hydraCoreRemoteObject{Type: typeName, Tag: tag, Raw: rawObject})
	}
	return objects, nil
}

var hydraCoreLocalAuthorityFields = map[string]bool{
	"bind_interface": true, "routing_mark": true, "netns": true,
	"protect_path": true, "domain_resolver": true, "plugin": true,
	"plugin_opts": true, "command": true, "executable": true,
	"config_path": true, "database_path": true, "state_directory": true,
	"socket_path": true, "certificate_path": true, "key_path": true,
}

func rejectLocalAuthorityFields(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if hydraCoreLocalAuthorityFields[key] {
				return &hydraCorePolicyError{code: "local_authority_field", path: childPath, message: "field requires local client authority"}
			}
			if err := rejectLocalAuthorityFields(child, childPath); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := rejectLocalAuthorityFields(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRemoteReferenceGraph(inbounds, outbounds, endpoints []hydraCoreRemoteObject) error {
	objects := make(map[string]hydraCoreRemoteObject, len(outbounds)+len(endpoints))
	for _, object := range append(append([]hydraCoreRemoteObject{}, outbounds...), endpoints...) {
		if _, exists := objects[object.Tag]; exists {
			return &hydraCorePolicyError{code: "duplicate_tag", path: "$", message: "outbound and endpoint tags must be unique"}
		}
		objects[object.Tag] = object
	}
	graph := make(map[string][]string, len(objects))
	for tag, object := range objects {
		references, err := remoteObjectReferences(object.Raw, "$")
		if err != nil {
			return err
		}
		for _, reference := range references {
			if _, exists := objects[reference]; !exists {
				return &hydraCorePolicyError{code: "missing_reference", path: "$", message: "remote object references a tag outside its resource graph"}
			}
		}
		graph[tag] = references
	}
	for _, inbound := range inbounds {
		references, err := remoteObjectReferences(inbound.Raw, "$")
		if err != nil {
			return err
		}
		for _, reference := range references {
			if _, exists := objects[reference]; !exists {
				return &hydraCorePolicyError{code: "missing_reference", path: "$", message: "remote inbound references a tag outside its resource graph"}
			}
		}
	}
	visiting := make(map[string]bool)
	visited := make(map[string]bool)
	var visit func(string) error
	visit = func(tag string) error {
		if visiting[tag] {
			return &hydraCorePolicyError{code: "reference_cycle", path: "$", message: "remote reference graph contains a cycle"}
		}
		if visited[tag] {
			return nil
		}
		visiting[tag] = true
		for _, reference := range graph[tag] {
			if err := visit(reference); err != nil {
				return err
			}
		}
		visiting[tag] = false
		visited[tag] = true
		return nil
	}
	for tag := range graph {
		if err := visit(tag); err != nil {
			return err
		}
	}
	return nil
}

func remoteObjectReferences(raw map[string]any, path string) ([]string, error) {
	seen := make(map[string]bool)
	var references []string
	var collect func(any, string) error
	collect = func(value any, currentPath string) error {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				childPath := currentPath + "." + key
				switch key {
				case "detour", "outbound", "endpoint":
					if child == nil {
						continue
					}
					reference, loaded := child.(string)
					if !loaded {
						return &hydraCorePolicyError{code: "invalid_reference", path: childPath, message: "reference field must contain a tag"}
					}
					if reference != "" && !seen[reference] {
						seen[reference] = true
						references = append(references, reference)
					}
				case "outbounds":
					values, loaded := child.([]any)
					if !loaded {
						return &hydraCorePolicyError{code: "invalid_reference", path: childPath, message: "outbounds reference field must be an array"}
					}
					for _, item := range values {
						reference, loaded := item.(string)
						if !loaded {
							return &hydraCorePolicyError{code: "invalid_reference", path: childPath, message: "outbounds reference must contain tags"}
						}
						if reference != "" && !seen[reference] {
							seen[reference] = true
							references = append(references, reference)
						}
					}
				default:
					if err := collect(child, childPath); err != nil {
						return err
					}
				}
			}
		case []any:
			for index, child := range typed {
				if err := collect(child, fmt.Sprintf("%s[%d]", currentPath, index)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := collect(raw, path); err != nil {
		return nil, err
	}
	return references, nil
}

func rejectDuplicateJSONKeys(content []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, "$", 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return &hydraCorePolicyError{code: "invalid_json", path: "$", message: "JSON document contains trailing data"}
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, path string, depth int) error {
	if depth > 64 {
		return &hydraCorePolicyError{code: "json_too_deep", path: path, message: "JSON document exceeds the nesting limit"}
	}
	token, err := decoder.Token()
	if err != nil {
		return &hydraCorePolicyError{code: "invalid_json", path: path, message: "JSON document is malformed"}
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return &hydraCorePolicyError{code: "invalid_json", path: path, message: "JSON object is malformed"}
			}
			key, loaded := keyToken.(string)
			if !loaded {
				return &hydraCorePolicyError{code: "invalid_json", path: path, message: "JSON object key is invalid"}
			}
			if seen[key] {
				return &hydraCorePolicyError{code: "duplicate_json_key", path: path + "." + key, message: "JSON object contains a duplicate key"}
			}
			seen[key] = true
			if err := consumeJSONValue(decoder, path+"."+key, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	case '[':
		index := 0
		for decoder.More() {
			if err := consumeJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
			index++
		}
		_, err = decoder.Token()
	default:
		return &hydraCorePolicyError{code: "invalid_json", path: path, message: "JSON delimiter is invalid"}
	}
	if err != nil {
		return &hydraCorePolicyError{code: "invalid_json", path: path, message: "JSON container is malformed"}
	}
	return nil
}
