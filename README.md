algostrategy/
├── config/
│   └── config.json                # Configuration files
├── scripts/                       # Directory for Python scripts
│   ├── tvdata.py                  # Script for TV data processing
│   ├── tvdata_update.py           # Script for updating TV data
│   ├── indicatordata_all.py       # Script for handling all indicator data
│   ├── indicator_update.py        # Script for updating indicators
│   ├── datasampaling.py           # Script for data sampling
│   ├── optionbuying.py            # Script for option buying strategy
│   ├── trailing_sl.py             # Script for trailing stop-loss strategy
│
├── logs/                          # Log files
│   └── option_buying.log            # Example log file
│
├── tests/                         # Unit tests and test scripts
│   ├── test_tvdata.py             # Tests for tvdata.py
│   ├── test_indicator_update.py   # Tests for indicator_update.py
│   └── ...
├── Dockerfile                     # Dockerfile for containerization
├── .dockerignore                  # Files to ignore in Docker builds
├── README.md                      # Project documentation
└── requirements.txt               # Python dependencies
