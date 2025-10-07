pipeline {
    agent any
    parameters {
    string(name: 'FILE_KEY', defaultValue: '', description: 'key file : sample: /dataset/train_dataset_20250801_132000.csv')
    }

    stages {
        stage('Checkout') {
            steps {
                // Checkout the code from the repository
                git branch: 'estimmo_cicd',credentialsId: 'github-pat', url: 'https://github.com/LyXoR51/my_certification_projects_lead.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    // Build the Docker image using the Dockerfile
                    sh 'docker build -t ml-pipeline-image .'
                }
            }
        }

        stage('Run Tests Inside Docker Container') {
            steps {
                withCredentials([
                    string(credentialsId: 'AWS_ACCESS_KEY_ID', variable: 'AWS_ACCESS_KEY_ID'),
                    string(credentialsId: 'AWS_SECRET_ACCESS_KEY', variable: 'AWS_SECRET_ACCESS_KEY'),
                    ]) {
                    script {
                        // Write environment variables to a temporary file
                        writeFile file: 'env.list', text: """
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
FILE_KEY=${params.FILE_KEY}
"""
                    }

                    // Run a temporary Docker container and pass env variables securely via --env-file
                    sh '''
docker run --rm --env-file env.list \
ml-pipeline-image \
bash -c "pytest --maxfail=1 --disable-warnings"
'''
                }
            }
        }
    }

    post {
        always {
            // Clean up workspace and remove dangling Docker images
            sh 'docker system prune -f'
            sh 'rm -f env.list'
        }
        success {
            echo 'Pipeline completed successfully!'
        }
        failure {
            echo 'Pipeline failed. Check logs for errors.'
        }
    }
}