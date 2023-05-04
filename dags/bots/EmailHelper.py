import smtplib
from bots.LoginInfo import LoginInfo

def send():
    try:
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(LoginInfo.EMAIL, LoginInfo.PASSWORD)
        subject="Testing"
        body_text="Testing surces"
        message="Subject: {}\n\n{}".format(subject, body_text)
        x.sendmail(LoginInfo.EMAIL, LoginInfo.EMAIL, message)
        print("SUCCESS")
    except Exception as exception:
        print(exception)
        print("FAILURE")

if __name__ == "__main__":
    send()