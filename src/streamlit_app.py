import streamlit as st
from src.tabs.engagement_tab.ui import render_engagement_tab
from src.tabs.game_tab.ui import render_game_info_tab

def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(page_title="Steam Reviews Analysis", layout="wide")
    
    # Add custom CSS for better aesthetics
    st.markdown("""
    <style>
        /* Fonts and general styling */
        .stApp {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        /* Main title and headers */
        h1, h2, h3 {
            color: #1E3A8A;
            font-weight: 600;
        }
        
        /* Container styling */
        .stContainer, div.block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* Card styling for metrics */
        div[data-testid="stMetric"] {
            background-color: #F8FAFC;
            border-radius: 8px;
            padding: 1rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            border-left: 4px solid #1E88E5;
        }
        
        div[data-testid="stMetric"] > div:nth-child(1) {
            color: #64748B;
        }
        
        div[data-testid="stMetric"] > div:nth-child(2) {
            color: #0F172A;
            font-weight: 600;
        }
        
        /* Button styling */
        .stButton button {
            background-color: #1E88E5;
            color: white;
            border-radius: 6px;
            padding: 0.25rem 1rem;
            font-weight: 500;
            transition: all 0.2s;
        }
        
        .stButton button:hover {
            background-color: #1565C0;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        }
        
        /* Input fields */
        .stTextInput input, .stDateInput input, .stSelectbox select {
            border-radius: 6px;
            border: 1px solid #CBD5E1;
        }
        
        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 1rem;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 0.5rem 1rem;
            border-radius: 6px 6px 0 0;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #E0F2FE !important;
            color: #0369A1 !important;
            border-left: 2px solid #0369A1;
            border-top: 2px solid #0369A1;
            border-right: 2px solid #0369A1;
            border-bottom: none;
        }
        
        /* Charts and plots */
        div[data-testid="stPlotlyChart"] {
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            padding: 1rem;
            margin-bottom: 1rem;
        }
        
        /* Review cards styling */
        div.element-container div.stMarkdown div p {
            margin-bottom: 0.5rem;
        }
        
        /* Expander styling */
        .streamlit-expanderHeader {
            font-weight: 500;
            color: #334155;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("Steam Game Reviews Analysis")
    
    # Create tabs
    tab1, tab2 = st.tabs(["Engagement", "Game Info"])
    
    with tab1:
        render_engagement_tab()
    
    with tab2:
        render_game_info_tab()

if __name__ == "__main__":
    main() 