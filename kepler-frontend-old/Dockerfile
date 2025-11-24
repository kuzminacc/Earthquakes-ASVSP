FROM nginx:alpine

# Copy the HTML file
COPY index.html /usr/share/nginx/html/
COPY kepler.gl.css /usr/share/nginx/html/

# Copy custom nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8082

CMD ["nginx", "-g", "daemon off;"]