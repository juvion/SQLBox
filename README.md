# SQLBox
**Convert a shared local drive to an automatic SQL database querying assistant.**

### Challenge  
The nature of our team requires constant SQL data queries, and very often the queries can take really long time. Standard laptop is limited for that purpose, since the querying job might get interrupted. On the other hand, the shared desktop runs Windows 7 only allows one user a time to access.

### Resources
- A desktop with Window 7 with python, Oracle SQL client installed.
- A shared LAN drive is assigned to the team, and each user has the ownership and permission of his/her folder. While administrator has access to all the folders. 

### Solution
A python code repetitively monitors any SQL query script changes saved to the LAN drive, and run the SQL queries whenever new scripts are added or changed.  Using Windows Task scheduler to run a python code at start. 

### Features
This tool supports multiple threads to run querying jobs.
