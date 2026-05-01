# VM Tunnel Keeper

يحافظ على اتصال SSH دائم ببيئة Replit NixOS وجهاز Ubuntu VM (QEMU) عبر Render.

## كيف يعمل

```
Render (هذا السيرفر)
  ├── SSH → Replit NixOS (المضيف)
  └── SSH → Ubuntu VM عبر port-forward من Replit (localhost:2222)
```

- يتصل بـ Replit عبر SSH بالمفتاح الخاص
- يفتح نفقًا (port forward) إلى Ubuntu VM على المنفذ 2222
- يرسل keepalive كل 30 ثانية لضمان بقاء الاتصال
- يعيد الاتصال تلقائيًا عند الانقطاع (كل 15 ثانية)

## المتغيرات البيئية (Render)

| المتغير | الوصف |
|---------|-------|
| `REPLIT_SSH_HOST` | عنوان SSH لـ Replit |
| `REPLIT_SSH_USER` | اسم المستخدم في Replit |
| `SSH_PRIVATE_KEY` | المفتاح الخاص كامل (بعلامات BEGIN/END) |
| `UBUNTU_SSH_PASSWORD` | كلمة مرور Ubuntu VM |

## لوحة التحكم

تعرض لوحة الويب:
- حالة كل اتصال (متصل / غير متصل / خطأ)
- إرسال أوامر تفاعلية لكل بيئة
- سجل كامل للأحداث
- إحصائيات إعادة الاتصال

## نشر على Render

1. أنشئ **Web Service** جديداً في [render.com](https://render.com)
2. اربطه بهذا الريبو: `github.com/hakercryptoplus-svg/vm-tunnel-keeper`
3. Build: `npm install` | Start: `node server.js`
4. أضف المتغيرات البيئية في لوحة Render
5. انشر!
