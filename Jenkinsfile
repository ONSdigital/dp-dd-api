#!groovy

node {
    stage('Checkout') {
        checkout scm
        sh 'git clean -dfx'
        sh 'git rev-parse --short HEAD > git-commit'
        sh 'set +e && (git describe --exact-match HEAD || true) > git-tag'
    }

    def revision = revisionFrom(readFile('git-tag').trim(), readFile('git-commit').trim())

    stage('Build') {
        sh 'sbt assembly'
    }

    stage('Test') {
        sh 'sbt "test-only scala.UnitTestRunner"'
    }

    stage('Image') {
        docker.withRegistry("https://${env.ECR_REPOSITORY_URI}", { ->
            sh '$(aws ecr get-login)'
            docker.build('dp-dd-database-loader', '--no-cache --pull --rm .').push(revision)
        })
    }
}

@NonCPS
def revisionFrom(tag, commit) {
    def matcher = (tag =~ /^release\/(\d+\.\d+\.\d+(?:-rc\d+)?)$/)
    matcher.matches() ? matcher[0][1] : commit
}
