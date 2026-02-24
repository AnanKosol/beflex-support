FROM nginx:1.25.4

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
COPY service.html /usr/share/nginx/html/service.html
COPY group-service.html /usr/share/nginx/html/group-service.html
COPY app.js /usr/share/nginx/html/app.js
COPY styles.css /usr/share/nginx/html/styles.css

EXPOSE 80
