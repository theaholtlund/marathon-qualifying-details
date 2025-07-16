# Marathon Qualifying Times

Hobby project for marathon qualifying times and details, by age and gender.

### Steps to Configure

1. Create a virtual environment:

```bash
python3 -m venv venv
```

2. Activate virtual environment on macOS:

```bash
source venv/bin/activate
```

3. Install the required packages:

```bash
pip install -r requirements.txt
```

4. Copy the `.env.template` file and rename it to `.env`:

```bash
cp .env.template .env
```

5. Open the environment file and fill in the required values as instructed.

6. Run the main script file:

```bash
python main.py
```

### Ideas for Later

- Add functionality for change tracking, to pull and alert on changes in the qualifying time for a given age group.
