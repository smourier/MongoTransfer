# MongoTransfer
A console utility to upsert documents from a MongoDB collection to another. Supports mirroring.

Example:

    MONGOTRANSFER.CLI -ind:MyDb -inc:MyColl -outcnx:mongodb+srv://myRemoteColl:MyPassword@whatever-xyzwz.gcp.mongodb.net/myDb?ssl=true

Copies all documents from local (default localhost:27017) database `MyDb`, collection `MyColl` to a MongoDb Atlas database.
