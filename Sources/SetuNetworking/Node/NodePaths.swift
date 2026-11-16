import Foundation
import Setu

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package enum NodePaths {
  package static var state: String {
    let configured = ProcessInfo.processInfo.environment["XDG_STATE_HOME"]
    let base =
      configured.flatMap { $0.hasPrefix("/") ? $0 : nil } ?? FileManager.default.homeDirectoryForCurrentUser.path
      + "/.local/state"
    return base + "/setu"
  }

  package static var socket: String {
    let environment = ProcessInfo.processInfo.environment
    if let socket = environment["SETU_SOCKET"] { return socket }
    if let configured = environment["XDG_RUNTIME_DIR"], configured.hasPrefix("/") {
      return configured + "/setu/control.sock"
    }
    #if os(Linux)
    return "/run/user/\(geteuid())/setu/control.sock"
    #else
    return state + "/run/control.sock"
    #endif
  }
}
