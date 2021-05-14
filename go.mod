module github.com/panjf2000/gnet

go 1.15

require (
	github.com/RussellLuo/timingwheel v0.0.0-20201029015908-64de9d088c74
	github.com/jackdai123/endpoint v0.0.0-20210426075941-c137c333103a
	github.com/panjf2000/ants/v2 v2.4.3
	github.com/valyala/bytebufferpool v1.0.0
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/sys v0.0.0-20210423082822-04245dca01da
)

//replace github.com/jackdai123/endpoint => ../endpoint
