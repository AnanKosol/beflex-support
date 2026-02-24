FROM nginx:1.25.4

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
COPY service.html /usr/share/nginx/html/service.html
COPY group-service.html /usr/share/nginx/html/group-service.html
COPY app.js /usr/share/nginx/html/app.js
COPY app.v2.js /usr/share/nginx/html/app.v2.js
COPY app.v3.js /usr/share/nginx/html/app.v3.js
COPY app.v4.js /usr/share/nginx/html/app.v4.js
COPY app.v5.js /usr/share/nginx/html/app.v5.js
COPY styles.css /usr/share/nginx/html/styles.css
COPY styles.v2.css /usr/share/nginx/html/styles.v2.css
COPY styles.v3.css /usr/share/nginx/html/styles.v3.css

EXPOSE 80
