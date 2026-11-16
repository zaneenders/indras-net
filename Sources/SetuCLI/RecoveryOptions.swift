import ArgumentParser
import Setu
import SetuNetworking

struct RecoveryOptions: ParsableArguments {
  @Option(
    name: .customLong("download-recovery-mib"),
    help: "Total retained download budget in MiB; 0 disables recovery. Verified files expire after 15 minutes.")
  var downloadRecoveryMiB: Int64 = defaultDownloadRecoveryBytes / 1_048_576

  func validate() throws(any Error) {
    guard (0...(Int64.max / 1_048_576)).contains(downloadRecoveryMiB) else {
      throw ValidationError("--download-recovery-mib must be nonnegative and fit in a signed 64-bit byte count")
    }
  }

  var bytes: Int64 { downloadRecoveryMiB * 1_048_576 }

  var arguments: [String] {
    bytes == defaultDownloadRecoveryBytes ? [] : ["--download-recovery-mib", String(downloadRecoveryMiB)]
  }
}
