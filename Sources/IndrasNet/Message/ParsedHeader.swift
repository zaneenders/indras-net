struct ParsedHeader: Sendable {
  var type: MessageType
  var payloadLength: UInt32
}
