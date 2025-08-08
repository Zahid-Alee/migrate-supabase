# Use Node.js image from the official Docker repository
FROM node:16

# Set working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json first to leverage Docker caching
COPY package*.json ./

# Install dependencies (including supabase-js and other required modules)
RUN npm install

# Copy the rest of your application code into the container
COPY . .

# Expose the port the app will run on (for debugging purposes)
EXPOSE 4000

# Command to run the migration or discovery script
CMD ["node", "discover-files.js"]
