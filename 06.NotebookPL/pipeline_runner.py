# Set input parameters
name = "Hasnain"
age = "7"

# Run notebook n1 and get the message
message = dbutils.notebook.run("/Workspace/n1", timeout_seconds=60, arguments={
    "name": name,
    "age": age
})

print("Returned message from n1:", message)

# Run notebook n2 and pass the message
dbutils.notebook.run("/Workspace/n2", timeout_seconds=60, arguments={
    "message": message
})
