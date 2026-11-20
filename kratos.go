package async

import "github.com/go-kratos/kratos/v2/transport"

// Runtime can be passed directly to kratos.Server. Kratos owns its lifecycle;
// register handlers before app.Run and let the app call Start and Stop.
var _ transport.Server = (*Runtime)(nil)
