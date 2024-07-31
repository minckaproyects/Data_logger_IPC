## SOP
Refer to https://drive.google.com/file/d/1nfBFTHKqQ91_JFEDn63AvFtVZ1f81i5l/view?usp=sharing on how to operate the code !

## Latest update

Addition of robutst error handling psycopg2.DatabaseError and reconection inside the loop with connect_to_db() as shown below.


```python
 try:
        execute_values(cursor, query, data_to_insert)
        cnx.commit()
        print("Uploaded data ")
    except psycopg2.DatabaseError as e:
        print(f"Error during data insertion: {e}")
        print("Attempting to reconnect...")
        cnx = connect_to_db()
        if cnx is not None:
            cursor = cnx.cursor()
            try:
                execute_values(cursor, query, data_to_insert)
                cnx.commit()
                print("Uploaded data after reconnection")
            except psycopg2.DatabaseError as e:
                print(f"Reconnection and data insertion failed: {e}")
        else:
            print("Reconnection failed. Skipping this batch of data.")

    values = ""
    loop += 1
```