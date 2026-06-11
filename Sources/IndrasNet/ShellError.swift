public enum ShellError: Error {
  case noListenPort

  public var errorDescription: String? {
    switch self {
    case .noListenPort: "no port bound"
    }
  }
}
