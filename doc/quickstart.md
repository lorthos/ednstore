## Quickstart


Create an instance of the DB and initialize

    (def db (SimpleDiskStore.))
    (initialize! db some-config)
    
Write data

    (insert! db "database-name" "key" "value")
    (insert! db "database-name" :key {:value :map})
    
Value can be any value type, and its not designed to have references inside data like atoms etc.. 
    
Delete data

    (delete! db "database-name" "key")
    
Read data

    (lookup db "key")
    
    
Stop the database

    (stop! db)
    
    
Take alook at the tests for more examples