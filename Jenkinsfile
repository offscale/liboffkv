pipeline {
  agent {
    docker {
      image 'gcc:8.3'
      args '--network host'
    }

  }
  stages {
    stage('install-deps') {
      steps {
        sh 'rm -rf build'
        sh '''if [[ ! -e pkgs ]]; then
	git clone https://github.com/microsoft/vcpkg.git pkgs
	cd pkgs
	bash ./bootstrap-vcpkg.sh
fi

'''
        sh '''cp -r vcpkg/ports/ pkgs
cd pkgs
./vcpkg upgrade --no-dry-run
./vcpkg install ppconsul etcdpp zkpp'''
      }
    }
    stage('build') {
      steps {
        sh '''ls
ls pkgs
ls pkgs/downloads/tools
'''
        sh '''WD=$(pwd)
CM=pkgs/downloads/tools/cmake-3.14.0-linux/cmake-3.14.0-Linux-x86_64/bin
export PATH="$PATH:$WD/$CM"
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug \\
      -DCMAKE_TOOLCHAIN_FILE="../pkgs/scripts/buildsystems/vcpkg.cmake" \\
      -DBUILD_TESTS=ON ..
cmake --build .'''
      }
    }
    stage('test') {
      steps {
        sh 'make test'
      }
    }
  }
  post {
    success {
      telegramSend "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' ${env.BUILD_URL}"

    }

    failure {
      telegramSend "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' ${env.BUILD_URL}"
    }

  }
}
