import psycopg2

from fastapi import FastAPI
from pydantic import BaseModel, Field

# Hello Hatch dev team! 
#
# I worked on this over the weekend, I had to make a few assumptions but if you have any questions on
# how or why I did something I would be happy to help. My email is maximilliancoburn@gmail.com. I come from
# a java and .NET background so a lot of this is new to me, please excuse any silly stylistic mistakes or simple 
# overlooked optimizations. Through this process I tried to use AI as sparingly as possible so you could get a feel
# for how I actually code. AI is an important tool, but I think its important to show what I can do without AI. 
# Any spot I used AI I marked down. Along with that I have somewhat over-commented the code, mostly to show my thought process.

# I have changed the test cases a bit to more clearly show conversation flow and how it is agrigated. Let me know if you
# have any questions. Thank you for taking the time to look this over, have a good rest of your day!

# This project leverages psycopg2, fastAPI, and uvicorn

# Connect
conn = psycopg2.connect(
    database="messaging_service",
    user="messaging_user",
    password="messaging_password",
    host="0.0.0.0"
)

# Open cursor into db
cur = conn.cursor()

# Initial db setup
cur.execute("""
DROP TABLE messages CASCADE;
DROP TABLE attachments CASCADE;

CREATE TABLE messages (
  id                    SERIAL PRIMARY KEY,
  m_from                TEXT,
  m_to                  TEXT,
  type                  TEXT,
  xillio_id             TEXT,
  messaging_provider_id TEXT,
  body                  TEXT,
  timestamp             TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE attachments (
  id          SERIAL PRIMARY KEY,
  message_id  int,
  text        TEXT,
  FOREIGN KEY (message_id) REFERENCES messages(id)
);           
""")
conn.commit()

app = FastAPI()

class Message(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    type: str = None
    messaging_provider_id: str = None
    xillio_id: str = None
    body: str = None
    attachments: list[str] = None
    timestamp: str

# Convert from object to db entry. In prod I would probably go with a true ORM solution
def add_message_to_db(message: Message):

    # Add message entry to the message table, saving the id to use as a foreign key to the attachments table
    cur.execute(f"""INSERT INTO messages (m_from, m_to, type, messaging_provider_id, xillio_id, body, timestamp) VALUES 
                   ('{message.from_}', '{message.to}', '{message.type}','{message.messaging_provider_id}',
                    '{message.xillio_id}','{message.body}', '{message.timestamp}') 
                   RETURNING id;""")
    
    # Check if we have any attachments, if so add them to the attachments table with the correct message foreign key
    if message.attachments != None:
        message_id = cur.fetchone()[0] # Found using AI
        for entry in message.attachments:
            cur.execute(f"INSERT INTO attachments (message_id, text) VALUES ({message_id}, '{entry}');")

    conn.commit()

@app.post("/api/messages/sms")
def sms(message: Message):
    add_message_to_db(message)
    return message

@app.post("/api/messages/email")
def email(message: Message):
    message.type = 'email' # Todo: Debatable if we want this, depends on the greater API ecosystem
    add_message_to_db(message)
    return message

@app.post("/api/webhooks/sms")
def web_hook_sms(message: Message):
    add_message_to_db(message)
    return message

@app.post("/api/webhooks/email")
def web_hook_sms(message: Message):
    message.type = 'xillio' # Todo: Debatable if we want this, depends on the greater API ecosystem
    add_message_to_db(message)
    return message

# For when printing out conversations 
def conversation_pretty_row(row: list):
    # retrieve all attachments based on foreign key
    cur.execute(f"SELECT text FROM attachments WHERE message_id = {row[0]}")
    attachments = cur.fetchall()

    return { 
        'message_id':   row[0],
        'from':         row[1],
        'to':           row[2],
        'type':         row[3],
        'xillio_id':    row[4],
        'messaging_provider_id': row[5],
        'body':         row[6],
        'attachments':  attachments,
        'timestamp':    row[7],
    }

# Note: Test cases have been modified to happen one day after the other so the date flow is more obvious 
@app.get("/api/conversations")
def conversations():

    conversation_lists = []
    current_conversation_list = []

    # This sql will group by to and from and sort on timestamp (LEAST/GREATEST pattern found using AI)
    cur.execute("SELECT * FROM messages ORDER BY LEAST(m_to, m_from), GREATEST(m_to, m_from), timestamp;")
    rows = cur.fetchall()

    if len(rows) == 0:
        return conversation_lists
    
    curr_from = rows[0][1]
    curr_to = rows[0][2]

    for row in rows:
        # Identify if we are in the same conversation 
        if (curr_from == row[1] or curr_from == row[2]) and (curr_to == row[1] or curr_to == row[2]): 
            current_conversation_list.append(conversation_pretty_row(row))
        # If we are no longer in the same conversation at that conversation list to the main list and restart the process
        else:
            conversation_lists.append(current_conversation_list)

            curr_from = row[1]
            curr_to = row[2]

            current_conversation_list = []
            current_conversation_list.append(conversation_pretty_row(row))
    
    # check if we have any conversations left, if so, add it to the main list
    if(len(current_conversation_list) > 0):
        conversation_lists.append(current_conversation_list)

    return conversation_lists

