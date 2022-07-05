# DataBase

Library to manage databases (sql and no sql) with python.
For now only provide a class to operate with a mongo database. 

# Dependencies
- **Pymongo** --> pip install pymongo==4.0.1

## Use of SELECT

Now if you want to use select operation, you should close the cursor given from the function, so, in order to implement these new feature on
V4 use **with** statement. Let's see an example:<br/>

*select function return MyCursor object. These object store the information from the query and the connection with the database*<br/>
**query = db.select({}, COLLECTION, fields={"Timestamp"}, sort_fields=[("Timestamp", ASCENDING)], limit=20)**<br/><br/>
*Using with statement the user can get cursor object and iterate over documents*<br/>
**with query as cursor:**<br/>
        **for doc in cursor:**<br/>
              **print(doc)**

*When with statement finish, the connection and the cursor will be closed automatically*
