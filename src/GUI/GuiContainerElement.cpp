/*
 * GuiContainerElement.cpp
 *
 *  Created on: Mar 25, 2015
 *      Author: bog
 */

#include <boglfw/GUI/GuiContainerElement.h>
#include <boglfw/GUI/GuiHelper.h>
#include <boglfw/GUI/GuiTheme.h>
#include <boglfw/GUI/GridLayout.h>
#include <boglfw/renderOpenGL/Viewport.h>
#include <boglfw/renderOpenGL/Shape2D.h>
#include <boglfw/renderOpenGL/RenderHelpers.h>
#include <boglfw/utils/assert.h>
#include <boglfw/renderOpenGL/glToolkit.h>

#include <glm/vec3.hpp>

#include <algorithm>

GuiContainerElement::GuiContainerElement()
	: layout_(new GridLayout()) {
	layout_->setOwner(this);
	onSizeChanged.add(std::bind(&GuiContainerElement::onSizeChangedHandler, this, std::placeholders::_1));
}

GuiContainerElement::~GuiContainerElement() {
	clear();
}

bool GuiContainerElement::containsPoint(glm::vec2 const& p) const {
	if (!transparentBackground_)
		return true;
	else {
		glm::vec2 clientPos = p - computedClientAreaOffset_;
		std::shared_ptr<GuiBasicElement> crt = GuiHelper::getTopElementAtPosition(*this, clientPos.x, clientPos.y);
		return crt != nullptr;
	}
}

void GuiContainerElement::draw(RenderContext const& ctx, glm::vec2 frameTranslation, glm::vec2 frameScale) {
	checkGLError("before GuiContainerElement::draw()");
	// draw background:
	if (!transparentBackground_) {
		Shape2D::get()->drawRectangle(frameTranslation, computedSize(), GuiTheme::getContainerFrameColor());
		Shape2D::get()->drawRectangleFilled(frameTranslation + glm::vec2{1, 1}, computedSize() - glm::vec2{2, 2}, GuiTheme::getContainerBackgroundColor());
	}

#ifdef DEBUG
	// draw client area frame
	//Shape2D::get()->drawRectangle(frameTranslation + computedClientAreaOffset_, computedClientAreaSize_, {1.f, 0.f, 1.f});
#endif

	// set clipping to visible client portion
	setClipping();
	// draw all children relative to the client area
	frameTranslation += computedClientAreaOffset_;
	for (auto &e : children_) {
		if (!e->isVisible())
			continue;
		RenderHelpers::flushAll();
		e->draw(ctx, frameTranslation + e->computedPosition(), frameScale);
		checkGLError("after GUI element draw");
	}
	// TODO draw frame around focused element:
	resetClipping();
}

void GuiContainerElement::setClipping() {
	// TODO
}

void GuiContainerElement::resetClipping() {
	// TODO
}

void GuiContainerElement::onSizeChangedHandler(glm::vec2 size) {
	updateClientArea();
	refreshLayout();
}

void GuiContainerElement::updateClientArea() {
	computedClientAreaOffset_ = clientAreaOffset_.get(computedSize());
	computedClientAreaCounterOffset_ = clientAreaCounterOffset_.get(computedSize());
	computedClientAreaSize_ = computedSize() - computedClientAreaOffset_ - computedClientAreaCounterOffset_;
}

void GuiContainerElement::addElement(std::shared_ptr<GuiBasicElement> e) {
	children_.push_back(e);
	e->setParent(this);
	e->setCaptureManager(getCaptureManager());
	refreshLayout();
}

void GuiContainerElement::removeElement(std::shared_ptr<GuiBasicElement> e) {
	assertDbg(e && e->parent_ == this);
	auto it = std::find(children_.begin(), children_.end(), e);
	assertDbg(it != children_.end());
	e->setParent(nullptr);
	children_.erase(it);
	refreshLayout();
}

/*void GuiContainerElement::mouseDown(MouseButtons button) {
	GuiBasicElement::mouseDown(button);
	if (elementUnderMouse_) {
		if (elementUnderMouse_ != focusedElement_) {
			if (focusedElement_)
				focusedElement_->focusLost();
			focusedElement_ = elementUnderMouse_;
			focusedElement_->focusGot();
		}
		elementUnderMouse_->mouseDown(button);
	}
}

void GuiContainerElement::mouseUp(MouseButtons button) {
	GuiBasicElement::mouseUp(button);
	if (elementUnderMouse_)
		elementUnderMouse_->mouseUp(button);
}

void GuiContainerElement::mouseMoved(glm::vec2 delta, glm::vec2 position) {
	GuiBasicElement::mouseMoved(delta, position);
	glm::vec2 clientPos = position - clientAreaOffset_;
	std::shared_ptr<GuiBasicElement> crt = GuiHelper::getTopElementAtPosition(children_, clientPos.x, clientPos.y);
	if (crt != elementUnderMouse_) {
		if (elementUnderMouse_)
			elementUnderMouse_->mouseLeave();
		elementUnderMouse_ = crt;
		if (elementUnderMouse_)
			elementUnderMouse_->mouseEnter();
	}
	if (elementUnderMouse_)
		elementUnderMouse_->mouseMoved(delta, clientPos - elementUnderMouse_->getPosition());
}

void GuiContainerElement::clicked(glm::vec2 clickPosition, MouseButtons button) {
	GuiBasicElement::clicked(clickPosition, button);
	if (elementUnderMouse_)
		elementUnderMouse_->clicked(clickPosition - clientAreaOffset_ - elementUnderMouse_->getPosition(), button);
}

bool GuiContainerElement::keyDown(int keyCode) {
	if (focusedElement_)
		return focusedElement_->keyDown(keyCode);
	else
		return false;
}

bool GuiContainerElement::keyUp(int keyCode) {
	if (focusedElement_)
		return focusedElement_->keyUp(keyCode);
	else
		return false;
}

bool GuiContainerElement::keyChar(char c) {
	if (focusedElement_)
		return focusedElement_->keyChar(c);
	else
		return false;
}*/

void GuiContainerElement::setClientArea(gfcoord offset, gfcoord counterOffset) {
	clientAreaOffset_.assignValue(offset);
	clientAreaCounterOffset_.assignValue(counterOffset);

	updateClientArea();
	refreshLayout();
}

void GuiContainerElement::getComputedClientArea(glm::vec2 &outOffset, glm::vec2 &outSize) const {
	outOffset = computedClientAreaOffset_;
	outSize = computedClientAreaSize_;
}

void GuiContainerElement::clear() {
	for (auto &c : children_)
		c->setParent(nullptr);
	children_.clear();
}

void GuiContainerElement::useLayout(std::shared_ptr<Layout> layout) {
	layout_->setOwner(nullptr);
	assertDbg(layout != nullptr);
	layout_ = layout;
	layout_->setOwner(this);
	refreshLayout();
}

void GuiContainerElement::refreshLayout() {
	layout_->update(children_.begin(), children_.end(), computedClientAreaSize_);
}
