# Use the official R image
# https://hub.docker.com/_/r-base
FROM r-base

RUN apt-get update -qq && apt-get install -y \
  git-core \
  libssl-dev \
  libcurl4-gnutls-dev \
  libsodium-dev \
  libglpk-dev

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy local code to the container image.
COPY . .

# Install any R packages
RUN Rscript -e "install.packages(c('plumber', 'jsonlite', 'slam', 'ggplot2', 'dplyr', 'broom', 'pastecs', 'car', 'rrcov', 'ROI', 'ROI.plugin.glpk', 'ROI.plugin.qpoases', 'ROI.plugin.optimx', 'ROI.plugin.lpsolve', 'ROI.plugin.quadprog', 'ROI.plugin.nloptr', 'ROI.plugin.ecos'))"

EXPOSE 8080

# Run the web service on container startup.
CMD [ "Rscript", "server.R"]
