import streamlit as st 
import requests
import os
import time 
import pandas as pd
# from api_utils import api_call 
user_access_token = os.environ.get("DATABRICKS_TOKEN")
host_name = "https://adb-640321604414221.1.azuredatabricks.net"
space_id='01f0a27dc74b1eb4a86f10883167c037'
Conversation_url = f"{host_name}/api/2.0/genie/spaces/01f0a27dc74b1eb4a86f10883167c037/start-conversation"
# result_url = f"{host_name}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"


st.title ("GENIE")
# API call to the genie space to 
def send_user_query(user_input):
    response = requests.post(
            Conversation_url,
            headers={
                "Authorization": f"Bearer {user_access_token}",
                "Content-Type": "application/json"
            },
            json={"content": user_input}
        )
    conversation_id = response.json().get("conversation_id")
    message_id = response.json().get("message_id")

    return conversation_id,message_id
# fetch the status of the message send to Giene space 
def fetch_question_response(conversation_id, message_id): 
    response_url =  f"{host_name}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    query_response = requests.get(
                response_url,
                headers={
                    "Authorization": f"Bearer {user_access_token}",
                    "Content-Type": "application/json"
                }
                
            )
    return query_response
def fetch_results(conversation_id,message_id,attachment_id): 
    execute_url = f"{host_name}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query"
    results = requests.post(
                execute_url,
                headers={
                    "Authorization": f"Bearer {user_access_token}",
                    "Content-Type": "application/json"
                }
                
            )
    return results
def get_data (conversation_id,message_id,attachment_id): 
    get_data_url =f"{host_name}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
    results = requests.get(
                get_data_url,
                headers={
                    "Authorization": f"Bearer {user_access_token}",
                    "Content-Type": "application/json"
                }
                
            )
    return results


user_input = st.text_input("Enter your question here")

if st.button("QUERY"): 
    if not user_input.strip(): 
        st.warning("Please enter a question")
    else:
        with st.spinner("Querying..."):
            conversation_id,message_id = send_user_query(user_input)

            query_response = fetch_question_response(conversation_id, message_id)
            conversation_id = query_response.json().get("conversation_id")
            message_id = query_response.json().get("message_id")
            while True : 
                query_response = fetch_question_response(conversation_id, message_id)
                data=query_response.json()
                time.sleep(5)
                with st.spinner(data.get("status")):
                    if data.get("status") in("COMPLETED","FAILED "):
                        break

            st.write(data.get("attachments")[0]["query"]["query"])
            conversation_id = data.get("conversation_id")
            message_id = data.get("message_id")
            # st.write(data)
            attachment_id = data.get("attachments")[0]["attachment_id"]
            result_response = fetch_results(conversation_id,message_id,attachment_id)
            # st.write(result_response.json())

            result_fetch = get_data(conversation_id,message_id,attachment_id)
            # st.write(result_fetch.json())
            # result_response = fetch_results(conversation_id
            while True : 
                query_response = get_data(conversation_id, message_id,attachment_id)
                data=query_response.json()
                # st.write(data)
            #     time.sleep(5)
            #     # st.write(data.get("status"))
                state = data["statement_response"]["status"]["state"]
                # st.write(state)
                if state in("SUCCEEDED","FAILED "):
                    break
                        
            # st.write(data)
            columns = data["statement_response"]["manifest"]["schema"]["columns"]
            col_names = [col["name"] for col in columns]
            rows = data["statement_response"]["result"]["data_array"]

            # Convert to Pandas DataFrame for Streamlit
            df = pd.DataFrame(rows, columns=col_names)
            st.dataframe(df)


           

        
