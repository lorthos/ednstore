# Writes

Writes happen in constant time as its an append to the active segment. Key and value can be any Clojure entity.

EdnStore uses nippy for serialization at the moment, but it should be trivial to make this layer pluggable.


