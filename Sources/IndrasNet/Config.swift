public struct Config: Sendable {
  public enum Mode: Sendable {
    case serve
    case connect
  }

  public let mode: Mode
  public let host: String
  public let port: Int
  public let jsonEventLog: Bool
  public let clientScript: [ClientScriptStep]

  public init(
    mode: Mode = .serve,
    host: String,
    port: Int,
    jsonEventLog: Bool = false,
    clientScript: [ClientScriptStep] = ClientScriptStep.defaultPingHello
  ) {
    self.mode = mode
    self.host = host
    self.port = port
    self.jsonEventLog = jsonEventLog
    self.clientScript = clientScript
  }
}
