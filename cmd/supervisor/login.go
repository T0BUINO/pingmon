package main

const loginHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>登录 · PingMon</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f1f5f9; color: #0f172a; }
    main { width: min(400px, calc(100% - 32px)); padding: 32px; background: #fff; border: 1px solid #e2e8f0; border-radius: 18px; box-shadow: 0 18px 48px rgba(15, 23, 42, .1); }
    h1 { margin: 0 0 8px; font-size: 26px; }
    p { margin: 0 0 24px; color: #64748b; }
    label { display: block; margin: 16px 0 6px; font-size: 14px; font-weight: 650; }
    input { width: 100%; padding: 11px 12px; border: 1px solid #cbd5e1; border-radius: 9px; font: inherit; }
    input:focus { outline: 3px solid #bfdbfe; border-color: #2563eb; }
    button { width: 100%; margin-top: 24px; padding: 11px; border: 0; border-radius: 9px; background: #2563eb; color: #fff; font: inherit; font-weight: 700; cursor: pointer; }
    .error { min-height: 20px; margin: 14px 0 -6px; color: #dc2626; font-size: 14px; }
  </style>
</head>
<body>
  <main>
    <h1>PingMon</h1>
    <p>登录后查看监控面板</p>
    <form method="post" action="/login">
      <input type="hidden" name="next" value="{{NEXT}}">
      <label for="username">用户名</label>
      <input id="username" name="username" autocomplete="username" required autofocus>
      <label for="password">密码</label>
      <input id="password" name="password" type="password" autocomplete="current-password" required>
      <div class="error" role="alert">{{ERROR}}</div>
      <button type="submit">登录</button>
    </form>
  </main>
</body>
</html>`
