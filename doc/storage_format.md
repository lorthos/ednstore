# Storage

EdnStore has a log-structured storage system. You can create as many databases as you like. Each database consists of at least one log file(s) called "segment".

## Disk layout
    + storage_root_folder
    + + database_folder
    + + + segment_file1
    + + + segment_file2

##Active Segment
  As the name suggest, this is the current file that ednstore writes to. All writes are appended to the current active segment.

  When the active segment reaches a certain size, a new segment is created. The new segment is becomes the active segment and the previous active segment becomes a read only segment.

##Read-Only Segment

  A Read Only segment is a former active segment without a write channel. 


##Storage Format on Disk


A write log looks like the following:

    key_length:key:operation_type:value_length:value

A delete log looks like the following:

    key_length:key:operation_type