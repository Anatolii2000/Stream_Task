from st_aggrid import AgGrid
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from PIL import Image
import io


#st.set_page_config(layout='wide') #Choose wide mode as the default setting

#Add a logo (optional) in the sidebar
#logo = Image.open(r'example.png')
#st.sidebar.image(logo,  width=120)

#Add the expander to provide some information about the app
#with st.sidebar.expander("About the App"):
 #    st.write("""
  #      This interactive project management App was built by Sharone Li using Streamlit. You can use the app to easily and quickly generate a Gannt chart for any project plan and management purposes. \n  \nYou can edit the project plan within the app and instantly generate and update the Gantt chart. You can also export the Gantt chart to png file and share it with your team very easily.)
   #  """)

#Create a user feedback section to collect comments and ratings from users
#with st.sidebar.form(key='columns_in_form',clear_on_submit=True): #set clear_on_submit=True so that the form will be reset/cleared once it's submitted
#    st.write('Please help us improve!')
#    st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style>', unsafe_allow_html=True) #Make horizontal radio buttons
#    rating=st.radio("Please rate the app",('1','2','3','4','5'),index=4)    #Use radio buttons for ratings
#    text=st.text_input(label='Please leave your feedback here') #Collect user feedback
#    submitted = st.form_submit_button('Submit')
#    if submitted:
#      st.write('Thanks for your feedback!')
#      st.markdown('Your Rating:')
#      st.markdown(rating)
#      st.markdown('Your Feedback:')
#      st.markdown(text)

#Add an app title. Use css to style the title
# st.markdown("""<style> .font {
#    font-size:30px ; font-family: 'Cooper Black'; color: #FF9633;}
#    </style> , unsafe_allow_html=True""")

#st.markdown('<p class="font">Upload your project plan file and generate Gantt chart instantly</p>', unsafe_allow_html=True)

#Add a template screenshot as an example
#st.subheader('Step 1: Download the project plan template')
#image = Image.open(r'example.png') #template screenshot provided as an example
#st.image(image,  caption='Make sure you use the same column names as in the template')

#Allow users to download the template
@st.cache
def convert_df(df):
     return df.to_csv().encode('utf-8')
df=pd.read_csv('example1.csv')
csv = convert_df(df)
st.download_button(
     label="Download Template",
     data=csv,
     file_name='template1.csv',
     mime='text/csv',
 )
# Add a file uploader to allow users to upload their project plan file
#st.subheader('Step 2: Upload your project plan file')

uploaded_file = st.file_uploader(
    "Fill out the project plan template and upload your file here. After you upload the file, you can edit your project plan within the app.",
    type=['csv'])
if uploaded_file is not None:
    Tasks = pd.read_csv(uploaded_file)
    Tasks['Start'] = Tasks['Start'].astype('datetime64[D]')
    Tasks['Finish'] = Tasks['Finish'].astype('datetime64[D]')

    grid_response = AgGrid(
        Tasks,
        editable=True,
        height=300,
        width='100%',
    )

    updated = grid_response['data']
    df = pd.DataFrame(updated)

else:
    st.warning('You need to upload a csv file.')

    # Main interface - section 3
st.subheader('Step 3: Generate the Gantt chart')

Options = st.selectbox("View Gantt Chart by:", ['Team', 'Completion Pct'], index=0)
if st.button('Generate Gantt Chart'):
    fig = px.timeline(
        df,
        x_start="Start",
        x_end="Finish",
        y="Task",
        color=Options,
        hover_name="Task Description"
    )

    fig.update_yaxes(
        autorange="reversed")  # if not specified as 'reversed', the tasks will be listed from bottom up

    fig.update_layout(
        title='Project Plan Gantt Chart',
        hoverlabel_bgcolor='#DAEEED',
        # Change the hover tooltip background color to a universal light blue color. If not specified, the background color will vary by team or completion pct, depending on what view the user chooses
        bargap=0.2,
        height=600,
        xaxis_title="",
        yaxis_title="",
        title_x=0.5,  # Make title centered
        xaxis=dict(
            tickfont_size=15,
            tickangle=270,
            rangeslider_visible=True,
            side="top",  # Place the tick labels on the top of the chart
            showgrid=True,
            zeroline=True,
            showline=True,
            showticklabels=True,
            tickformat="%x\n",
            # Display the tick labels in certain format. To learn more about different formats, visit: https://github.com/d3/d3-format/blob/main/README.md#locale_format
        )
    )

    fig.update_xaxes(tickangle=0, tickfont=dict(family='Rockwell', color='blue', size=15))

    st.plotly_chart(fig, use_container_width=True)  # Display the plotly chart in Streamlit

"""    st.subheader(
            'Bonus: Export the interactive Gantt chart to HTML and share with others!')  # Allow users to export the Plotly chart to HTML
    buffer = io.StringIO()
    fig.write_html(buffer, include_plotlyjs='cdn')
    html_bytes = buffer.getvalue().encode()
    st.download_button(
        label='Export to HTML',
        data=html_bytes,
        file_name='Gantt.html',
        mime='text/html'
    )
else:
    st.write('---')
"""