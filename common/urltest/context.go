package urltest

import "context"

type contextKeyIsUnifiedDelay struct{}
type contextKeyDisableBackgroundChecks struct{}

func ContextWithIsUnifiedDelay(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyIsUnifiedDelay{}, true)
}

func IsUnifiedDelayFromContext(ctx context.Context) bool {
	return ctx.Value(contextKeyIsUnifiedDelay{}) != nil
}

func ContextWithDisableBackgroundChecks(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyDisableBackgroundChecks{}, true)
}

func BackgroundChecksDisabled(ctx context.Context) bool {
	return ctx.Value(contextKeyDisableBackgroundChecks{}) != nil
}
