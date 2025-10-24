package model

// Case class for chunk data - must be defined at the top level for serialization
case class ChunkData(idx: Int, startIndex: Int, endIndex: Int, text: String)
