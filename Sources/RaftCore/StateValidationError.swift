/// An invalid durable consensus image, independent of the storage backend.
public enum StateValidationError: Error, Equatable {
  case invalidState
}
