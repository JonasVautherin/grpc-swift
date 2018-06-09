import Foundation
import SwiftProtobuf
import SwiftGRPC
import NIO
import NIOHTTP1

// Processes individual gRPC messages and stream-close events on a HTTP2 channel.
public protocol GRPCCallHandler: ChannelHandler {
  func makeGRPCServerCodec() -> ChannelHandler
}

// Provides `GRPCCallHandler` objects for the methods on a particular service name.
public protocol CallHandlerProvider {
  var serviceName: String { get }
  
  func handleMethod(_ methodName: String, headers: HTTPHeaders, serverHandler: GRPCChannelHandler, ctx: ChannelHandlerContext) -> GRPCCallHandler?
}

// Listens on a newly-opened HTTP2 channel and waits for the request headers to become available.
// Once those are available, asks the `CallHandlerProvider` corresponding to the request's service name for an
// `GRPCCallHandler` object. That object is then forwarded the individual gRPC messages.
public final class GRPCChannelHandler: ChannelInboundHandler {
  public typealias InboundIn = RawGRPCServerRequestPart
  
  public typealias OutboundOut = RawGRPCServerResponsePart
  
  fileprivate let servicesByName: [String: CallHandlerProvider]
  
  public init(servicesByName: [String: CallHandlerProvider]) {
    self.servicesByName = servicesByName
  }
  
  //! FIXME: Avoid the dependency on SwiftGRPC's `Metadata` object.
  public func sendStatus(_ status: ServerStatus, ctx: ChannelHandlerContext) -> EventLoopFuture<Void> {
    let promise: EventLoopPromise<Void> = ctx.eventLoop.newPromise()
    sendStatus(status, ctx: ctx, promise: promise)
    return promise.futureResult
  }
  
  public func sendStatus(_ status: ServerStatus, ctx: ChannelHandlerContext, promise: EventLoopPromise<Void>?) {
    var trailers = HTTPHeaders(status.trailingMetadata.dictionaryRepresentation.map { ($0, $1) })
    trailers.add(name: "grpc-status", value: String(describing: status.code.rawValue))
    trailers.add(name: "grpc-message", value: status.message)
    
    return ctx.writeAndFlush(self.wrapOutboundOut(.trailers(trailers)), promise: promise)
  }
  
  public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
    let requestPart = self.unwrapInboundIn(data)
    switch requestPart {
    case .headers(let headers):
      let uriComponents = headers.uri.components(separatedBy: "/")
      guard uriComponents.count >= 3 && uriComponents[0].isEmpty,
        let providerForServiceName = servicesByName[uriComponents[1]],
        let callHandler = providerForServiceName.handleMethod(uriComponents[2], headers: headers.headers, serverHandler: self, ctx: ctx) else {
          sendStatus(ServerStatus(code: .unimplemented, message: "unknown method " + headers.uri),
                     ctx: ctx, promise: nil)
          return
      }
      
      var responseHeaders = HTTPHeaders()
      responseHeaders.add(name: "content-type", value: "application/grpc")
      ctx.write(self.wrapOutboundOut(.headers(responseHeaders)), promise: nil)
      
      let codec = callHandler.makeGRPCServerCodec()
      _ = ctx.pipeline.add(handler: codec, after: self)
        .then { ctx.pipeline.add(handler: callHandler, after: codec) }
        .then { ctx.pipeline.remove(handler: self) }
      
    case .message, .end:
      print("received \(requestPart), should have been removed as a handler at this point")
      break
    }
  }
}
