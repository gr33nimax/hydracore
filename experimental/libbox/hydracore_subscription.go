package libbox

import (
	"encoding/json"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	C "github.com/sagernet/sing-box/constant"
	subscriptioncontract "github.com/sagernet/sing-box/contract/subscription"

	"golang.org/x/mod/semver"
)

const (
	hydraSubscriptionAPIVersion = "hydra.io/subscription/v2"
	hydraSubscriptionKind       = "Subscription"
	hydraSubscriptionMaxBytes   = 12 * 1024 * 1024
	hydraSubscriptionMaxSeq     = uint64(9_007_199_254_740_991)
)

var hydraSubscriptionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var hydraExtensionPattern = regexp.MustCompile(`^[a-z0-9]+(?:[.-][a-z0-9]+)+/v[1-9][0-9]*$`)

type hydraSubscriptionV2 struct {
	APIVersion        string                        `json:"api_version"`
	Kind              string                        `json:"kind"`
	Identity          hydraSubscriptionIdentity     `json:"identity"`
	Validity          hydraSubscriptionValidity     `json:"validity"`
	Display           *hydraSubscriptionDisplay     `json:"display,omitempty"`
	Requirements      hydraSubscriptionRequirements `json:"requirements"`
	Update            *hydraSubscriptionUpdate      `json:"update,omitempty"`
	Resources         []hydraSubscriptionResource   `json:"resources"`
	Profiles          []hydraSubscriptionProfile    `json:"profiles"`
	DefaultProfile    string                        `json:"default_profile,omitempty"`
	RequiredExtension []string                      `json:"required_extensions,omitempty"`
	Extensions        map[string]json.RawMessage    `json:"extensions,omitempty"`
}

type hydraSubscriptionIdentity struct {
	Issuer   string `json:"issuer"`
	ID       string `json:"id"`
	Channel  string `json:"channel,omitempty"`
	Sequence uint64 `json:"sequence"`
}

type hydraSubscriptionValidity struct {
	IssuedAt  string `json:"issued_at"`
	NotBefore string `json:"not_before,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type hydraSubscriptionDisplay struct {
	Name       json.RawMessage `json:"name,omitempty"`
	Homepage   string          `json:"homepage,omitempty"`
	SupportURL string          `json:"support_url,omitempty"`
}

type hydraSubscriptionRequirements struct {
	Core   hydraSubscriptionCoreRequirements   `json:"core"`
	Client hydraSubscriptionClientRequirements `json:"client"`
}

type hydraSubscriptionCoreRequirements struct {
	ID           string   `json:"id"`
	APIVersion   int      `json:"api_version"`
	VersionRange string   `json:"version_range,omitempty"`
	RemotePolicy int      `json:"remote_policy"`
	Features     []string `json:"features,omitempty"`
}

type hydraSubscriptionClientRequirements struct {
	SubscriptionContract int      `json:"subscription_contract"`
	MinVersion           string   `json:"min_version,omitempty"`
	Features             []string `json:"features,omitempty"`
}

type hydraSubscriptionUpdate struct {
	URL                    string `json:"url,omitempty"`
	MinimumIntervalSeconds int    `json:"minimum_interval_seconds,omitempty"`
}

type hydraSubscriptionResource struct {
	ID                   string          `json:"id"`
	Format               string          `json:"format"`
	RequestedPermissions []string        `json:"requested_permissions,omitempty"`
	Document             json.RawMessage `json:"document"`
}

type hydraSubscriptionProfile struct {
	ID               string                        `json:"id"`
	Resource         string                        `json:"resource"`
	Name             json.RawMessage               `json:"name"`
	Entrypoint       hydraSubscriptionProfileEntry `json:"entrypoint"`
	Enabled          *bool                         `json:"enabled,omitempty"`
	RequiredFeatures []string                      `json:"required_features,omitempty"`
}

type hydraSubscriptionProfileEntry struct {
	Section string `json:"section"`
	Tag     string `json:"tag"`
}

type hydraSubscriptionInspection struct {
	SchemaVersion int                             `json:"schema_version"`
	Valid         bool                            `json:"valid"`
	Identity      *hydraSubscriptionIdentity      `json:"identity,omitempty"`
	Validity      *hydraSubscriptionValidity      `json:"validity,omitempty"`
	Requirements  *hydraSubscriptionRequirements  `json:"requirements,omitempty"`
	Resources     []hydraSubscriptionResourceInfo `json:"resources"`
	Profiles      []hydraSubscriptionProfileInfo  `json:"profiles"`
	Diagnostics   []hydraCoreValidationDiagnostic `json:"diagnostics"`
}

type hydraSubscriptionResourceInfo struct {
	ID                   string   `json:"id"`
	RequestedPermissions []string `json:"requested_permissions"`
	Protocols            []string `json:"protocols"`
}

type hydraSubscriptionProfileInfo struct {
	ID       string `json:"id"`
	Resource string `json:"resource"`
	Section  string `json:"section"`
	Enabled  bool   `json:"enabled"`
}

type hydraSubscriptionResourceIndex struct {
	tags      map[string]map[string]string
	protocols []string
}

func HydraCoreSubscriptionSchema() string { return subscriptioncontract.PlainSchema() }

func HydraCoreSubscriptionJWESchema() string { return subscriptioncontract.JWESchema() }

func HydraCoreSubscriptionJWEPolicy() string {
	return `{"schema_version":1,"serialization":"flattened-json","alg":"dir","enc":"A256GCM","typ":"hydra-subscription+jwe","cty":"application/vnd.hydra.subscription+json","key_bytes":32,"encrypted_key_bytes":0,"iv_bytes":12,"tag_bytes":16,"external_aad":false,"compression":false,"key_fragment":"hydra-key"}`
}

func HydraCoreValidateSubscription(content string) string {
	_, _, err := validateHydraSubscriptionV2(content)
	result := hydraCoreValidationResult{
		SchemaVersion: 1,
		Profile:       "subscription_v2",
		Valid:         err == nil,
		Diagnostics:   []hydraCoreValidationDiagnostic{},
	}
	if err != nil {
		appendHydraValidationError(&result, err)
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return `{"schema_version":1,"profile":"subscription_v2","valid":false,"diagnostics":[{"severity":"error","code":"internal_error","path":"$","message":"validation result could not be encoded"}]}`
	}
	return string(encoded)
}

func HydraCoreInspectSubscription(content string) string {
	document, indexes, err := validateHydraSubscriptionV2(content)
	inspection := hydraSubscriptionInspection{
		SchemaVersion: 1,
		Valid:         err == nil,
		Resources:     []hydraSubscriptionResourceInfo{},
		Profiles:      []hydraSubscriptionProfileInfo{},
		Diagnostics:   []hydraCoreValidationDiagnostic{},
	}
	if err != nil {
		result := hydraCoreValidationResult{Diagnostics: []hydraCoreValidationDiagnostic{}}
		appendHydraValidationError(&result, err)
		inspection.Diagnostics = result.Diagnostics
	} else {
		inspection.Identity = &document.Identity
		inspection.Validity = &document.Validity
		inspection.Requirements = &document.Requirements
		for _, resource := range document.Resources {
			protocols := append([]string(nil), indexes[resource.ID].protocols...)
			sort.Strings(protocols)
			inspection.Resources = append(inspection.Resources, hydraSubscriptionResourceInfo{
				ID:                   resource.ID,
				RequestedPermissions: append([]string(nil), resource.RequestedPermissions...),
				Protocols:            protocols,
			})
		}
		for _, profile := range document.Profiles {
			inspection.Profiles = append(inspection.Profiles, hydraSubscriptionProfileInfo{
				ID:       profile.ID,
				Resource: profile.Resource,
				Section:  profile.Entrypoint.Section,
				Enabled:  profile.Enabled == nil || *profile.Enabled,
			})
		}
	}
	encoded, marshalErr := json.Marshal(inspection)
	if marshalErr != nil {
		return `{"schema_version":1,"valid":false,"resources":[],"profiles":[],"diagnostics":[{"severity":"error","code":"internal_error","path":"$","message":"inspection result could not be encoded"}]}`
	}
	return string(encoded)
}

func appendHydraValidationError(result *hydraCoreValidationResult, err error) {
	if policyErr, loaded := err.(*hydraCorePolicyError); loaded {
		result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
			Severity: "error",
			Code:     policyErr.code,
			Path:     policyErr.path,
			Message:  policyErr.message,
		})
		return
	}
	result.Diagnostics = append(result.Diagnostics, hydraCoreValidationDiagnostic{
		Severity: "error",
		Code:     "subscription_invalid",
		Path:     "$",
		Message:  "subscription failed validation",
	})
}

func validateHydraSubscriptionV2(content string) (*hydraSubscriptionV2, map[string]hydraSubscriptionResourceIndex, error) {
	if len(content) > hydraSubscriptionMaxBytes {
		return nil, nil, &hydraCorePolicyError{code: "document_too_large", path: "$", message: "subscription exceeds the plaintext size limit"}
	}
	if err := rejectDuplicateJSONKeys([]byte(content)); err != nil {
		return nil, nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var document hydraSubscriptionV2
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, &hydraCorePolicyError{code: "invalid_subscription_shape", path: "$", message: "subscription does not match the strict v2 shape"}
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, nil, &hydraCorePolicyError{code: "invalid_subscription_shape", path: "$", message: "subscription must contain exactly one JSON document"}
	}
	if document.APIVersion != hydraSubscriptionAPIVersion || document.Kind != hydraSubscriptionKind {
		return nil, nil, &hydraCorePolicyError{code: "unsupported_subscription_version", path: "$.api_version", message: "subscription discriminator is not Hydra Subscription v2"}
	}
	if err := validateHydraSubscriptionIdentity(document.Identity); err != nil {
		return nil, nil, err
	}
	if err := validateHydraSubscriptionValidity(document.Validity); err != nil {
		return nil, nil, err
	}
	if err := validateHydraSubscriptionDisplay(document.Display); err != nil {
		return nil, nil, err
	}
	if err := validateHydraSubscriptionRequirements(document.Requirements); err != nil {
		return nil, nil, err
	}
	if err := validateHydraSubscriptionUpdate(document.Update); err != nil {
		return nil, nil, err
	}
	if err := validateHydraSubscriptionExtensions(document.RequiredExtension, document.Extensions); err != nil {
		return nil, nil, err
	}
	if len(document.Resources) == 0 || len(document.Resources) > 64 {
		return nil, nil, &hydraCorePolicyError{code: "invalid_resource_count", path: "$.resources", message: "subscription must contain between 1 and 64 resources"}
	}
	indexes := make(map[string]hydraSubscriptionResourceIndex, len(document.Resources))
	for index, resource := range document.Resources {
		path := "$.resources[" + jsonNumber(index) + "]"
		if !validHydraSubscriptionID(resource.ID) {
			return nil, nil, &hydraCorePolicyError{code: "invalid_resource_id", path: path + ".id", message: "resource id is invalid"}
		}
		if _, exists := indexes[resource.ID]; exists {
			return nil, nil, &hydraCorePolicyError{code: "duplicate_resource_id", path: path + ".id", message: "resource ids must be unique"}
		}
		resourceIndex, err := validateHydraSubscriptionResource(resource, path)
		if err != nil {
			return nil, nil, err
		}
		indexes[resource.ID] = resourceIndex
	}
	if err := validateHydraSubscriptionProfiles(document, indexes); err != nil {
		return nil, nil, err
	}
	return &document, indexes, nil
}

func validateHydraSubscriptionIdentity(identity hydraSubscriptionIdentity) error {
	issuer, err := url.Parse(identity.Issuer)
	if err != nil || issuer.Scheme != "https" || issuer.Host == "" || issuer.User != nil || (issuer.Path != "" && issuer.Path != "/") || issuer.RawQuery != "" || issuer.Fragment != "" {
		return &hydraCorePolicyError{code: "invalid_issuer", path: "$.identity.issuer", message: "issuer must be an HTTPS origin without credentials, query, or fragment"}
	}
	if !validHydraSubscriptionID(identity.ID) {
		return &hydraCorePolicyError{code: "invalid_subscription_id", path: "$.identity.id", message: "subscription id is invalid"}
	}
	if identity.Channel != "" && !validHydraSubscriptionID(identity.Channel) {
		return &hydraCorePolicyError{code: "invalid_channel", path: "$.identity.channel", message: "subscription channel is invalid"}
	}
	if identity.Sequence > hydraSubscriptionMaxSeq {
		return &hydraCorePolicyError{code: "invalid_sequence", path: "$.identity.sequence", message: "subscription sequence exceeds the portable integer limit"}
	}
	return nil
}

func validateHydraSubscriptionValidity(validity hydraSubscriptionValidity) error {
	issuedAt, err := time.Parse(time.RFC3339, validity.IssuedAt)
	if err != nil {
		return &hydraCorePolicyError{code: "invalid_issued_at", path: "$.validity.issued_at", message: "issued_at must be an RFC 3339 timestamp"}
	}
	var notBefore time.Time
	if validity.NotBefore != "" {
		notBefore, err = time.Parse(time.RFC3339, validity.NotBefore)
		if err != nil {
			return &hydraCorePolicyError{code: "invalid_not_before", path: "$.validity.not_before", message: "not_before must be an RFC 3339 timestamp"}
		}
	}
	if validity.ExpiresAt != "" {
		expiresAt, parseErr := time.Parse(time.RFC3339, validity.ExpiresAt)
		if parseErr != nil {
			return &hydraCorePolicyError{code: "invalid_expires_at", path: "$.validity.expires_at", message: "expires_at must be an RFC 3339 timestamp"}
		}
		lowerBound := issuedAt
		if !notBefore.IsZero() && notBefore.After(lowerBound) {
			lowerBound = notBefore
		}
		if !expiresAt.After(lowerBound) {
			return &hydraCorePolicyError{code: "invalid_validity_window", path: "$.validity", message: "expires_at must be after the start of the validity window"}
		}
	}
	return nil
}

func validateHydraSubscriptionDisplay(display *hydraSubscriptionDisplay) error {
	if display == nil {
		return nil
	}
	if len(display.Name) > 0 {
		if err := validateHydraLocalizedText(display.Name, "$.display.name"); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		path  string
		value string
	}{{"$.display.homepage", display.Homepage}, {"$.display.support_url", display.SupportURL}} {
		if field.value != "" && !validHydraHTTPSURL(field.value) {
			return &hydraCorePolicyError{code: "invalid_https_url", path: field.path, message: "URL must use HTTPS and contain no credentials or fragment"}
		}
	}
	return nil
}

func validateHydraSubscriptionRequirements(requirements hydraSubscriptionRequirements) error {
	if requirements.Core.ID != "io.hydrabox.hydracore" {
		return &hydraCorePolicyError{code: "incompatible_core", path: "$.requirements.core.id", message: "subscription requires a different core"}
	}
	if requirements.Core.APIVersion > hydraCoreAPIVersion || requirements.Core.APIVersion < 1 {
		return &hydraCorePolicyError{code: "incompatible_core_api", path: "$.requirements.core.api_version", message: "subscription requires an unsupported HydraCore API"}
	}
	if requirements.Core.RemotePolicy != 2 {
		return &hydraCorePolicyError{code: "incompatible_remote_policy", path: "$.requirements.core.remote_policy", message: "subscription requires an unsupported remote policy"}
	}
	if requirements.Core.VersionRange != "" {
		matches, valid := hydraVersionMatchesRange(C.Version, requirements.Core.VersionRange)
		if !valid {
			return &hydraCorePolicyError{code: "invalid_core_version_range", path: "$.requirements.core.version_range", message: "core version range is invalid"}
		}
		if !matches {
			return &hydraCorePolicyError{code: "incompatible_core_version", path: "$.requirements.core.version_range", message: "subscription requires a different HydraCore version"}
		}
	}
	if requirements.Client.SubscriptionContract != 2 {
		return &hydraCorePolicyError{code: "incompatible_subscription_contract", path: "$.requirements.client.subscription_contract", message: "subscription requires a different contract version"}
	}
	if requirements.Client.MinVersion != "" && !semver.IsValid(normalizeSemver(requirements.Client.MinVersion)) {
		return &hydraCorePolicyError{code: "invalid_client_min_version", path: "$.requirements.client.min_version", message: "client minimum version must be semantic versioning"}
	}
	if err := validateHydraFeatureNames(requirements.Client.Features, "$.requirements.client.features"); err != nil {
		return err
	}
	supportedFeatures := map[string]bool{
		"rmux": true, "amnezia-3": true, "xhttp": true, "vless-encryption": true,
		"config-validation": true, "runtime-snapshot": true, "runtime-events": true,
		"managed-url-test-sessions": true,
	}
	if hydraCoreCallEnabled {
		supportedFeatures["call"] = true
		supportedFeatures["call_vk_parasite"] = true
		supportedFeatures["call_vk_eight_lane_kcp"] = true
		supportedFeatures["call_vk_pre_kcp_admission"] = true
		supportedFeatures["call_vk_relay_flow_control"] = true
	}
	seen := make(map[string]bool)
	for index, feature := range requirements.Core.Features {
		if seen[feature] {
			return &hydraCorePolicyError{code: "duplicate_required_feature", path: "$.requirements.core.features[" + jsonNumber(index) + "]", message: "required core features must be unique"}
		}
		seen[feature] = true
		if !supportedFeatures[feature] {
			return &hydraCorePolicyError{code: "unsupported_required_feature", path: "$.requirements.core.features[" + jsonNumber(index) + "]", message: "subscription requires an unsupported core feature"}
		}
	}
	return nil
}

func validateHydraFeatureNames(features []string, path string) error {
	seen := make(map[string]bool, len(features))
	for index, feature := range features {
		if !validHydraSubscriptionID(feature) {
			return &hydraCorePolicyError{code: "invalid_required_feature", path: path + "[" + jsonNumber(index) + "]", message: "required feature name is invalid"}
		}
		if seen[feature] {
			return &hydraCorePolicyError{code: "duplicate_required_feature", path: path + "[" + jsonNumber(index) + "]", message: "required features must be unique"}
		}
		seen[feature] = true
	}
	return nil
}

func hydraVersionMatchesRange(version string, versionRange string) (bool, bool) {
	normalizedVersion := normalizeSemver(version)
	if !semver.IsValid(normalizedVersion) {
		return false, false
	}
	clauses := strings.Fields(strings.ReplaceAll(versionRange, ",", " "))
	if len(clauses) == 0 || len(clauses) > 8 {
		return false, false
	}
	for _, clause := range clauses {
		operator := "="
		required := clause
		for _, candidate := range []string{">=", "<=", ">", "<", "="} {
			if strings.HasPrefix(clause, candidate) {
				operator = candidate
				required = strings.TrimPrefix(clause, candidate)
				break
			}
		}
		normalizedRequired := normalizeSemver(required)
		if required == "" || !semver.IsValid(normalizedRequired) {
			return false, false
		}
		comparison := semver.Compare(normalizedVersion, normalizedRequired)
		matches := comparison == 0
		switch operator {
		case ">=":
			matches = comparison >= 0
		case "<=":
			matches = comparison <= 0
		case ">":
			matches = comparison > 0
		case "<":
			matches = comparison < 0
		}
		if !matches {
			return false, true
		}
	}
	return true, true
}

func validateHydraSubscriptionUpdate(update *hydraSubscriptionUpdate) error {
	if update == nil {
		return nil
	}
	if update.URL != "" && !validHydraHTTPSURL(update.URL) {
		return &hydraCorePolicyError{code: "invalid_update_url", path: "$.update.url", message: "update URL must use HTTPS and contain no credentials or fragment"}
	}
	if update.MinimumIntervalSeconds != 0 && (update.MinimumIntervalSeconds < 300 || update.MinimumIntervalSeconds > 2_592_000) {
		return &hydraCorePolicyError{code: "invalid_update_interval", path: "$.update.minimum_interval_seconds", message: "update interval is outside the allowed range"}
	}
	return nil
}

func validateHydraSubscriptionExtensions(required []string, extensions map[string]json.RawMessage) error {
	seen := make(map[string]bool)
	for index, name := range required {
		if !hydraExtensionPattern.MatchString(name) || len(name) > 255 {
			return &hydraCorePolicyError{code: "invalid_extension_name", path: "$.required_extensions[" + jsonNumber(index) + "]", message: "required extension name is invalid"}
		}
		if seen[name] {
			return &hydraCorePolicyError{code: "duplicate_required_extension", path: "$.required_extensions", message: "required extensions must be unique"}
		}
		seen[name] = true
		if _, exists := extensions[name]; !exists {
			return &hydraCorePolicyError{code: "missing_required_extension", path: "$.extensions", message: "required extension payload is missing"}
		}
		return &hydraCorePolicyError{code: "unsupported_required_extension", path: "$.required_extensions[" + jsonNumber(index) + "]", message: "required extension is not supported by this core"}
	}
	for name := range extensions {
		if !hydraExtensionPattern.MatchString(name) || len(name) > 255 {
			return &hydraCorePolicyError{code: "invalid_extension_name", path: "$.extensions", message: "extension name is invalid"}
		}
	}
	return nil
}

func validateHydraSubscriptionResource(resource hydraSubscriptionResource, path string) (hydraSubscriptionResourceIndex, error) {
	if resource.Format != "sing-box-json" {
		return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "unsupported_resource_format", path: path + ".format", message: "resource format is not supported"}
	}
	if len(resource.Document) == 0 {
		return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "missing_resource_document", path: path + ".document", message: "resource document is required"}
	}
	if err := validateHydraRemoteConfigV2(string(resource.Document)); err != nil {
		return hydraSubscriptionResourceIndex{}, remapHydraPolicyError(err, path+".document")
	}
	if err := CheckConfig(string(resource.Document)); err != nil {
		return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "native_config_invalid", path: path + ".document", message: "resource failed native HydraCore validation"}
	}
	index, permissions, err := indexHydraSubscriptionResource(resource.Document, path+".document")
	if err != nil {
		return hydraSubscriptionResourceIndex{}, err
	}
	declared := make(map[string]bool)
	for permissionIndex, permission := range resource.RequestedPermissions {
		if declared[permission] {
			return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "duplicate_permission", path: path + ".requested_permissions[" + jsonNumber(permissionIndex) + "]", message: "requested permissions must be unique"}
		}
		declared[permission] = true
	}
	if len(declared) != len(permissions) {
		return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "permission_mismatch", path: path + ".requested_permissions", message: "requested permissions must exactly describe the resource authority"}
	}
	for permission := range permissions {
		if !declared[permission] {
			return hydraSubscriptionResourceIndex{}, &hydraCorePolicyError{code: "permission_mismatch", path: path + ".requested_permissions", message: "requested permissions must exactly describe the resource authority"}
		}
	}
	return index, nil
}

func indexHydraSubscriptionResource(content json.RawMessage, path string) (hydraSubscriptionResourceIndex, map[string]bool, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(content, &root); err != nil {
		return hydraSubscriptionResourceIndex{}, nil, &hydraCorePolicyError{code: "invalid_resource_document", path: path, message: "resource document is invalid"}
	}
	index := hydraSubscriptionResourceIndex{tags: map[string]map[string]string{"outbounds": {}, "endpoints": {}}}
	permissions := make(map[string]bool)
	for _, section := range []string{"inbounds", "outbounds", "endpoints"} {
		objects, err := decodeRemoteTypedObjects(root[section], path+"."+section)
		if err != nil {
			return hydraSubscriptionResourceIndex{}, nil, err
		}
		if len(objects) > 0 {
			switch section {
			case "inbounds":
				permissions["network.inbound.call"] = true
			case "outbounds":
				permissions["network.outbound"] = true
			case "endpoints":
				permissions["network.endpoint.wireguard"] = true
			}
		}
		for _, object := range objects {
			index.protocols = append(index.protocols, section+":"+object.Type)
			if section != "inbounds" {
				index.tags[section][object.Tag] = object.Type
			}
		}
	}
	return index, permissions, nil
}

func validateHydraSubscriptionProfiles(document hydraSubscriptionV2, indexes map[string]hydraSubscriptionResourceIndex) error {
	if len(document.Profiles) == 0 || len(document.Profiles) > 4096 {
		return &hydraCorePolicyError{code: "invalid_profile_count", path: "$.profiles", message: "subscription must contain between 1 and 4096 profiles"}
	}
	profiles := make(map[string]bool, len(document.Profiles))
	enabledProfiles := make(map[string]bool)
	for index, profile := range document.Profiles {
		path := "$.profiles[" + jsonNumber(index) + "]"
		if !validHydraSubscriptionID(profile.ID) {
			return &hydraCorePolicyError{code: "invalid_profile_id", path: path + ".id", message: "profile id is invalid"}
		}
		if profiles[profile.ID] {
			return &hydraCorePolicyError{code: "duplicate_profile_id", path: path + ".id", message: "profile ids must be unique"}
		}
		profiles[profile.ID] = true
		resourceIndex, exists := indexes[profile.Resource]
		if !exists {
			return &hydraCorePolicyError{code: "missing_profile_resource", path: path + ".resource", message: "profile references an unknown resource"}
		}
		if err := validateHydraLocalizedText(profile.Name, path+".name"); err != nil {
			return err
		}
		if profile.Entrypoint.Section != "outbounds" && profile.Entrypoint.Section != "endpoints" {
			return &hydraCorePolicyError{code: "invalid_profile_section", path: path + ".entrypoint.section", message: "profile entrypoint must be an outbound or endpoint"}
		}
		if _, exists := resourceIndex.tags[profile.Entrypoint.Section][profile.Entrypoint.Tag]; !exists {
			return &hydraCorePolicyError{code: "missing_profile_entrypoint", path: path + ".entrypoint.tag", message: "profile entrypoint does not resolve in its resource"}
		}
		if err := validateHydraFeatureNames(profile.RequiredFeatures, path+".required_features"); err != nil {
			return err
		}
		enabled := profile.Enabled == nil || *profile.Enabled
		if enabled {
			enabledProfiles[profile.ID] = true
		}
	}
	if len(enabledProfiles) == 0 {
		return &hydraCorePolicyError{code: "no_enabled_profiles", path: "$.profiles", message: "subscription must contain an enabled profile"}
	}
	if document.DefaultProfile != "" && !enabledProfiles[document.DefaultProfile] {
		return &hydraCorePolicyError{code: "invalid_default_profile", path: "$.default_profile", message: "default profile must reference an enabled profile"}
	}
	return nil
}

func validateHydraLocalizedText(content json.RawMessage, path string) error {
	if len(content) == 0 {
		return &hydraCorePolicyError{code: "missing_localized_text", path: path, message: "localized text is required"}
	}
	var text string
	if json.Unmarshal(content, &text) == nil {
		if text == "" || len(text) > 1024 {
			return &hydraCorePolicyError{code: "invalid_localized_text", path: path, message: "localized text is empty or too long"}
		}
		return nil
	}
	var localized map[string]string
	if err := json.Unmarshal(content, &localized); err != nil || localized["default"] == "" {
		return &hydraCorePolicyError{code: "invalid_localized_text", path: path, message: "localized text requires a non-empty default value"}
	}
	for _, value := range localized {
		if value == "" || len(value) > 1024 {
			return &hydraCorePolicyError{code: "invalid_localized_text", path: path, message: "localized text contains an empty or oversized value"}
		}
	}
	return nil
}

func validHydraSubscriptionID(value string) bool {
	return hydraSubscriptionIDPattern.MatchString(value)
}

func validHydraHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.Fragment == ""
}

func remapHydraPolicyError(err error, prefix string) error {
	policyErr, loaded := err.(*hydraCorePolicyError)
	if !loaded {
		return err
	}
	path := prefix
	if policyErr.path != "$" {
		path += strings.TrimPrefix(policyErr.path, "$")
	}
	return &hydraCorePolicyError{code: policyErr.code, path: path, message: policyErr.message}
}

func jsonNumber(value int) string {
	return strconv.Itoa(value)
}
