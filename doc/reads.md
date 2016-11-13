# Reads

Each segment has an in-memory index, index is a map of {:key :offset} pairs. 

Segments scanned in natural order (Newest segment first).

First match is returned since it should be the latest value for that key. A disk seek is required to lookup the value of an existing key.

