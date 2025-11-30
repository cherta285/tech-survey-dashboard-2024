# Data Dictionary

## Tables

### demographics
| Column | Type | Description |
|--------|------|-------------|
| ResponseId | INTEGER | Unique respondent ID |
| Country | STRING | Respondent country |
| Age | STRING | Age group |
| EdLevel | STRING | Education level |

### language_haveworked
| Column | Type | Description |
|--------|------|-------------|
| ResponseId | INTEGER | Respondent ID (FK) |
| Technology | STRING | Programming language name |