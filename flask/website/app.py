from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
import json
from config import Config
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__)
app.config.from_object(Config)
db = SQLAlchemy(app)

# Flask-Login 설정
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Editor 모델 정의
class Editor(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

    def get_id(self):
        return str(self.id)

    # UserMixin 메서드 명시적 정의
    def is_authenticated(self):
        return True  # 로그인된 사용자는 항상 인증됨

    def is_active(self):
        return self.is_active  # DB의 is_active 값 반환

    def is_anonymous(self):
        return False  # 익명 사용자가 아님

s3_client = boto3.client(
    's3',
    aws_access_key_id=app.config['AWS_ACCESS_KEY'],
    aws_secret_access_key=app.config['AWS_SECRET_KEY'],
    region_name=app.config['REGION']  # 예: 'us-east-1'
)

# 데이터베이스 초기화 (최초 실행 시 테이블 생성)
with app.app_context():
    db.create_all()

def get_filename_with_date_hour(prefix="articles", format=".txt"):
    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d%H")
    filename = f"{prefix}_{timestamp}{format}"
    print(f"생성된 파일명: {filename}")
    return filename

def get_prefix():
    filename = get_filename_with_date_hour()
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    prefix = f"FINAL/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/{filename}"
    print(f"생성된 S3 prefix: {prefix}")
    return prefix

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(Editor, int(user_id))

@app.route('/')
def home():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        editor = Editor.query.filter_by(username=username).first()
        
        if editor and check_password_hash(editor.password, password):
            if editor.is_active:
                login_user(editor)
                return redirect(url_for('editor_page'))
            else:
                flash('이 계정은 비활성화 상태입니다.', 'error')
        else:
            flash('아이디 또는 비밀번호가 잘못되었습니다.', 'error')
    
    return render_template('login.html')

@app.route('/editor', methods=['GET', 'POST'])
@login_required
def editor_page():
    if request.method == 'POST':
        text = request.form['text']
        try:
            # Lambda 함수 호출
            prefix = get_prefix()
            response = s3_client.put_object(
                Bucket=app.config['BUCKET_NAME'],
                Key=prefix,
                Body=text.encode("utf-8"),
                ContentType="text/plain"
            )

            flash(f'Lambda 호출 성공', 'success')
        except Exception as e:
            flash(f'Lambda 호출 실패: {str(e)}', 'error')
    
    return render_template('editor.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

if __name__ == '__main__':
   app.run('0.0.0.0', port=5000, debug=True)