FROM nginx:1.25.4

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
COPY service.html /usr/share/nginx/html/service.html
COPY group-service.html /usr/share/nginx/html/group-service.html
COPY user-csv-service.html /usr/share/nginx/html/user-csv-service.html
COPY pm.html /usr/share/nginx/html/pm.html
COPY audit.html /usr/share/nginx/html/audit.html
COPY query-sizing.html /usr/share/nginx/html/query-sizing.html
COPY support-other.html /usr/share/nginx/html/support-other.html
COPY app.js /usr/share/nginx/html/app.js
COPY app.v2.js /usr/share/nginx/html/app.v2.js
COPY app.v3.js /usr/share/nginx/html/app.v3.js
COPY app.v4.js /usr/share/nginx/html/app.v4.js
COPY app.v5.js /usr/share/nginx/html/app.v5.js
COPY pm-page.js /usr/share/nginx/html/pm-page.js
COPY audit-page.js /usr/share/nginx/html/audit-page.js
COPY query-sizing-page.js /usr/share/nginx/html/query-sizing-page.js
COPY sidebar-nav.js /usr/share/nginx/html/sidebar-nav.js
COPY styles.css /usr/share/nginx/html/styles.css
COPY styles.v2.css /usr/share/nginx/html/styles.v2.css
COPY styles.v3.css /usr/share/nginx/html/styles.v3.css
COPY samples /usr/share/nginx/html/samples

EXPOSE 80
