import ArgumentParser

struct UploadOptions: ParsableArguments {
  @Option(help: "Maximum simultaneous incoming uploads, including idle uploads.") var maxActiveUploads = 128
  @Option(help: "Expire inactive incoming uploads after this many seconds.") var uploadIdleTimeoutSeconds = 900

  func validate() throws(any Error) {
    guard maxActiveUploads > 0, uploadIdleTimeoutSeconds > 0 else {
      throw ValidationError("--max-active-uploads and --upload-idle-timeout-seconds must be positive")
    }
  }

  var arguments: [String] {
    var result: [String] = []
    if maxActiveUploads != 128 { result += ["--max-active-uploads", String(maxActiveUploads)] }
    if uploadIdleTimeoutSeconds != 900 {
      result += ["--upload-idle-timeout-seconds", String(uploadIdleTimeoutSeconds)]
    }
    return result
  }
}
