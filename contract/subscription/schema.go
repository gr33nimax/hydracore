package subscription

import _ "embed"

//go:embed schema/hydra-subscription-v2.schema.json
var plainSchema string

//go:embed schema/hydra-subscription-jwe-v2.schema.json
var jweSchema string

func PlainSchema() string { return plainSchema }

func JWESchema() string { return jweSchema }
