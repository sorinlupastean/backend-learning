# backend-learning

Un monorepo cu mai multe variante de backend, fiecare construită de la zero ca să învăț și să pun în practică conceptele.

## Structură

```
apps/
  <varianta>/   fiecare backend e un proiect separat, cu propriul package.json
```

## Comenzi

```bash
npm install          # instalează dependențele pentru toate aplicațiile
npm run dev          # pornește toate aplicațiile
npx turbo run dev --filter=<varianta>   # pornește doar una
npm run build
npm run test
```
