function onFormSubmit(e) {
  var form = FormApp.getActiveForm();
  var sheet = SpreadsheetApp.getActiveSheet();
  var range = e.range;
  var row = range.getRow();

  // FormResponse에서 응답 ID 가져오기
  var formId = "1AQNjnYoEX1gI39X9Z04YOiqR2CVBs2XwXnFYiBR-YqU"; // Google Forms URL에서 복사한 ID
  var form = FormApp.openById(formId);
  var responses = form.getResponses();
  var lastResponse = responses[responses.length - 1]; // 가장 최근 응답
  var responseId = lastResponse.getId(); // 응답 ID
  var timestamp = sheet.getRange(row, 1).getValue(); // 타임스탬프
  var email = sheet.getRange(row, 2).getValue(); // 이메일

  // 응답 ID를 Google Sheets에 기록 (예: 열 C)
  sheet.getRange(row, 3).setValue(responseId);

  var payload = {
    "responseId": responseId, // Google Forms의 응답 ID 사용
    "timestamp": timestamp.toISOString(),
    "email": email,
    "action": "INSERT_OR_UPDATE"
  };

  var options = {
    "method": "POST",
    "contentType": "application/json",
    "payload": JSON.stringify(payload)
  };

  var url = "https://fxpc6t9u24.execute-api.ap-northeast-2.amazonaws.com/dev/member/regist";
  UrlFetchApp.fetch(url, options);
}

// 트리거 설정
function createTrigger() {
  ScriptApp.newTrigger("onFormSubmit")
    .forSpreadsheet(SpreadsheetApp.getActiveSpreadsheet())
    .onFormSubmit()
    .create();
}