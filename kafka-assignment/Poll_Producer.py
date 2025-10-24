from poll_response_api import PollResponseAPI
import json

print(json.dumps(PollResponseAPI().survey_questions, indent=4))

print(PollResponseAPI().poll_response_api())