
TAG    			:= $$(git describe --tags)
REGISTRY		:= registry.spin.nersc.gov
PROJECT 		:= dmcreyno
REGISTRY_NAME	:= ${REGISTRY}/${PROJECT}/${IMG}

NAME_NOTEBOOK		:= splash_ml
IMG_NOTEBOOK   	:= ${NAME_NOTEBOOK}:${TAG}
REGISTRY_NOTEBOOK	:= ${REGISTRY}/${PROJECT}/${NAME_NOTEBOOK}:${TAG}

.PHONY: build

hello:
	@echo "Hello" ${REGISTRY}

build_notebook:
	@docker build -t ${IMG_NOTEBOOK} -f Dockerfile .
	@echo "tagging to: " ${IMG_NOTEBOOK}    ${REGISTRY_NOTEBOOK}
	@docker tag ${IMG_NOTEBOOK} ${REGISTRY_NOTEBOOK}


push_notebook:
	@echo "Pushing " ${REGISTRY_NOTEBOOK}
	@docker push ${REGISTRY_NOTEBOOK}

login:
	@docker log -u ${DOCKER_USER} -p ${DOCKER_PASS}
	