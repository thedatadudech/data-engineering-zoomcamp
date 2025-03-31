import streamlit as st
import sys

# Debug information
st.write("Python version:", sys.version)
st.write("Starting minimal test app...")

try:
    # Basic app content
    st.title("Test Application")
    st.write("Hello World!")

    # Add some interactive elements to test functionality
    if st.button("Click me"):
        st.success("Button clicked!")

    # Show some debug info
    st.info("Server is running correctly if you can see this message")

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.exception(e)