package common

// ResolveFunc — подмена DNS для signalling-клиентов.
type ResolveFunc func(hostname string) (string, error)
