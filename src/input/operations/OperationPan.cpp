/*
 * OperationPan.cpp
 *
 *  Created on: Nov 4, 2014
 *      Author: bog
 */
 
#ifdef WITH_BOX2D

#include <boglfw/input/operations/OperationPan.h>
#include <boglfw/input/operations/OperationContext.h>
#include <boglfw/renderOpenGL/Viewport.h>
#include <boglfw/renderOpenGL/Camera.h>

#include <GLFW/glfw3.h>
#include <glm/glm.hpp>

#ifdef DEBUG_DMALLOC
#include <dmalloc.h>
#endif

OperationPan::OperationPan(InputEvent::MOUSE_BUTTON assignedButton)
	: pContext(nullptr)
	, isFlyActive(false)
	, isDragging(false)
	, lastDelta{}
	, filterTimes{}
	, lastIndex(0)
	, frictionFactor(1.1)
	, flySpeed()
	, boundButton(assignedButton)
{
}

OperationPan::~OperationPan() {
}

void OperationPan::enter(const OperationContext* pContext) {
	this->pContext = pContext;
}
void OperationPan::leave() {
	this->pContext = nullptr;
}
void OperationPan::getFocus() {
}
void OperationPan::loseFocus() {
	isFlyActive = false;
	isDragging = false;
}

void OperationPan::handleInputEvent(InputEvent& ev) {
	switch (ev.type) {
	case InputEvent::EV_MOUSE_DOWN: {
		if (ev.mouseButton != boundButton)
			break;
		isDragging = true;
		isFlyActive = false;
		lastIndex = 0;
		lastDelta[lastIndex] = glm::vec2(ev.dx, ev.dy);
		filterTimes[lastIndex] = glfwGetTime();
		break;
	}
	case InputEvent::EV_MOUSE_UP: {
		if (ev.mouseButton != boundButton)
			break;
		isDragging = false;
		isFlyActive = true;
		// compute average drag over the last nFilter frames
		flySpeed = glm::vec2(0);
		float minTime = 1e+20;
		for (unsigned i=0; i<nFilter; i++) {
			flySpeed += lastDelta[i];
			if (filterTimes[i] < minTime)
				minTime = filterTimes[i];
		}
		flySpeed /= (glfwGetTime()- minTime) * pContext->pViewport->camera()->getOrthoZoom() * 2;
		flySpeed.x *= -1;
		break;
	}
	case InputEvent::EV_MOUSE_MOVED: {
		if (!isDragging)
			break;
		lastIndex = (lastIndex + 1) % nFilter;
		lastDelta[lastIndex] = glm::vec2(ev.dx, ev.dy);
		filterTimes[lastIndex] = glfwGetTime();
		pContext->pViewport->camera()->move(glm::vec3(-ev.dx, ev.dy, 0) / pContext->pViewport->camera()->getOrthoZoom());
		break;
	}
	case InputEvent::EV_MOUSE_SCROLL: {
		float factor = ev.dz < 0 ? 0.90f : 1.10f;
		pContext->pViewport->camera()->setOrthoZoom(pContext->pViewport->camera()->getOrthoZoom() * factor);
		break;
	}
	default:
		break;
	}
}

void OperationPan::update(float dt) {
	if (isFlyActive) {
		pContext->pViewport->camera()->move(glm::vec3{flySpeed * dt, 0.f});
		flySpeed /= frictionFactor;
		if (glm::length(flySpeed) * pContext->pViewport->camera()->getOrthoZoom() < 5) // less than 5 screen pixels per second, then stop
			isFlyActive = false;
	}
}

#endif // WITH_BOX2D