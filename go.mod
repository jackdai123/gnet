module github.com/jackdai123/gnet

go 1.15

require (
	github.com/RussellLuo/timingwheel v0.0.0-20201029015908-64de9d088c74
	github.com/jackdai123/endpoint v0.0.0-20210426075941-c137c333103a
	github.com/panjf2000/ants/v2 v2.4.4
	github.com/panjf2000/gnet v1.4.4
	github.com/valyala/bytebufferpool v1.0.0
	go.uber.org/zap v1.16.0
	golang.org/x/sys v0.0.0-20210514084401-e8d321eab015
)

//replace github.com/jackdai123/endpoint => ../endpoint
