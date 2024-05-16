import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders! :cup_with_straw:")
st.write(
    """Orders that need to filled."""
)

# Display the Fruit Options List in Your Streamlit in Snowflake (SiS) App. 
session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()
# st.dataframe(data=my_dataframe, use_container_width=True)

if my_dataframe:
    # Enable editing data in Streamlit app:
    editable_df = st.data_editor(my_dataframe)
    # Add a Submit Button:
    submitted = st.button('Submit')

    if submitted:
        # st.success("Someone clicked the button.", icon="👍")

        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)

        try:
            og_dataset.merge(edited_dataset
                                ,(og_dataset['name_on_order'] == edited_dataset['name_on_order'])
                                ,[when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                                )
            st.success('Order(s) Updated!', icon="👍")
        except:
            st.write('Someting went wrong.')

else:
    # Hide the table if there are no pending orders:
    st.success("There are no pending orders right now.", icon="👍")
