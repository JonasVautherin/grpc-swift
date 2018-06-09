import Foundation
import SwiftProtobuf
import SwiftGRPC
import NIO
import NIOHTTP1

// Provides a means for decoding incoming gRPC messages into protobuf objects, and exposes a promise that should be
// fulfilled when it is time to return a status to the client.
// Calls through to `processMessage` for individual messages it receives, which needs to be implemented by subclasses.
public class StatusSendingHandler<RequestMessage: Message, ResponseMessage: Message>: GRPCCallHandler, ChannelInboundHandler {
  public func makeGRPCServerCodec() -> ChannelHandler { return GRPCServerCodec<RequestMessage, ResponseMessage>() }
  
  public typealias InboundIn = GRPCServerRequestPart<RequestMessage>
  public typealias OutboundOut = GRPCServerResponsePart<ResponseMessage>
  
  let statusPromise: EventLoopPromise<ServerStatus>
  
  private(set) weak var ctx: ChannelHandlerContext?
  
  public init(eventLoop: EventLoop) {
    self.statusPromise = eventLoop.newPromise()
  }
  
  public func handlerAdded(ctx: ChannelHandlerContext) {
    self.ctx = ctx
    
    statusPromise.futureResult
      .mapIfError { ($0 as? ServerStatus) ?? .processingError }
      .whenSuccess { [weak self] in
        if let strongSelf = self,
          let ctx = strongSelf.ctx {
          strongSelf.sendStatus($0, ctx: ctx, promise: nil)
        }
    }
  }
  
  //! TODO: This method is almost identical to the one in `GRPCChannelHandler`; we could try to unify them.
  func sendStatus(_ status: ServerStatus, ctx: ChannelHandlerContext, promise: EventLoopPromise<Void>?) {
    var trailers = HTTPHeaders(status.trailingMetadata.dictionaryRepresentation.map { ($0, $1) })
    trailers.add(name: "grpc-status", value: String(describing: status.code.rawValue))
    trailers.add(name: "grpc-message", value: status.message)
    
    return ctx.writeAndFlush(self.wrapOutboundOut(.trailers(trailers)), promise: promise)
  }
  
  public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
    switch self.unwrapInboundIn(data) {
    case .headers: preconditionFailure("should not have received headers")
    case .message(let message): processMessage(message)
    case .end: endOfStreamReceived()
    }
  }
  
  public func processMessage(_ message: RequestMessage) {
    fatalError("needs to be overridden")
  }
  
  public func endOfStreamReceived() { }
}
