import sys

filepath = r'c:\Users\Admin\Documents\CHATBOT SNOWFLAKE\streamlit_app.py'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_idx = -1
end_idx = -1

for i, line in enumerate(lines):
    if '# FALLBACK: If DB is empty, try fetching from Yahoo Finance directly' in line and start_idx == -1:
        start_idx = i
    elif 'progress_bar.empty()' in line and start_idx != -1 and i > start_idx + 80 and end_idx == -1:
        end_idx = i
        break

if start_idx != -1 and end_idx != -1:
    print(f'Found block from {start_idx} to {end_idx}. Deleting...')
    del lines[start_idx:end_idx+1]
    with open(filepath, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print('Deleted successfully.')
else:
    print('Failed to find block boundaries.')
