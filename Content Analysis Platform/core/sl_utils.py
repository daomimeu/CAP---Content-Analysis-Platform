from collections import Counter


list_sl_cutes = ['curiosity', 'urgency', 'tone', 'emotion', 'specificity']
list_sl_length = ['length_long', 'length_med', 'length_short']
list_sl_binary = ['emoji', 'personalization', 'offer', 'product_name', 'feature', 'question', 'exclamation', 'ai']

list_sl_all = list_sl_cutes + list_sl_length + list_sl_binary

rec_mapper = {
    'feature': list_sl_cutes + list_sl_cutes + list_sl_length + list_sl_binary,
    'direction': [1]*5 + [-1]*5 + [1]*3 + [1]*8,
    'message': [
        'Make subject line more curious',
        'Make subject line more time-sensitive',
        'Make subject line more casual',
        'Make subject line more positive',
        'Make subject line more exclusive',

        'Make subject line more informative',
        'Make subject line more non-urgent',
        'Make subject line more formal',
        'Make subject line more negative (e.g. FOMO)',
        'Make subject line more generic',

        'Keep subject line within 50-60 characters',
        'Keep subject line within 35-50 characters',
        'Keep subject line within 20-35 characters',

        'Include emoji in subject line',
        'Include name personalization in subject line',
        'Include offer in subject line',
        'Include product name in subject line',
        'Include product features in subject line',
        'Use question in subject line',
        'Use exclamation in subject line',
        'Include word AI in subject line'
    ]
}

cutes_meaning = {
    'curiosity': {
        'positive': 'Spark interest by leaving things unsaid',
        'negative': 'Spark interest by explicitly stating information'
    },
    'urgency': {
        'positive': 'Use time-sensitive language to induce urgency',
        'negative': 'Reduce time-sensitivity in subject line'
    },
    'tone': {
        'positive': 'Use casual, informal and friendly language',
        'negative': 'Use formal language'
    },
    'emotion': {
        'positive': 'Focus on achieving aspirations or benefits',
        'negative': 'Focus on avoiding negative consequences'
    },
    'specificity': {
        'positive': 'Make subject line appeal to specific audience (e.g. based on hobbies, behaviors or personal traits)',
        'negative': 'Make subject line appeal to a generic audience'
    }
}


def get_top3_recommendations(df):
    """
    This function takes a dataframe with columns 'Approach', 'Best Performing', 
    'All Campaigns', and 'Difference', and returns the top 3 recommendations 
    based on the magnitude of the difference.
    
    Parameters:
    - df: pandas DataFrame containing 'Approach', 'Best Performing', 'All Campaigns', and 'Difference'.
    
    Returns:
    - A DataFrame with top 3 recommendations based on the 'Difference' column.
    """
    
    # Step 1: Sort the dataframe by the magnitude of the 'Difference' column (top 3)
    df_sorted = df.iloc[df['Difference'].abs().sort_values(ascending=False).index]

    # Get top 3 rows with the highest absolute difference
    df_top3 = df_sorted.head(3)

    # Step 2: Map the values in the 'Difference' column to cutes_meaning
    def get_meaning(row):
        approach = row['Approach']
        if row['Difference'] > 0:
            return f"{cutes_meaning[approach]['positive']}"
        else:
            return f"{cutes_meaning[approach]['negative']}"

    # Apply the function to create a new column 'Recommendation'
    df_top3['Recommendation'] = df_top3.apply(get_meaning, axis=1)

    return df_top3['Recommendation']