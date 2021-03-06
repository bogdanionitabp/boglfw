#include <boglfw/math/transform.h>

#include <glm/gtx/quaternion.hpp>

// set a new world position for the transform
void Transform::setPosition(glm::vec3 pos) {
	pos_ = pos;
	matDirty_ = true;
}

// set a new world orientation for the transform
void Transform::setOrientation(glm::quat orient) {
	orient_ = glm::normalize(orient);
	matDirty_ = true;
}

void Transform::lookAt(glm::vec3 wPos/*, glm::vec3 up*/) {
	glm::vec3 direction = glm::normalize(wPos - pos_);
	glm::vec3 z {0, 0, 1};
	if (direction == z) {
		orient_ = glm::quat{1,0,0,0};
	} else {
		glm::vec3 rotAxis = glm::normalize(glm::cross(z, direction));
		float angle = acosf(glm::dot(direction, z));
		orient_ = glm::angleAxis(angle, rotAxis);
	}
	matDirty_ = true;
}

void Transform::moveTo(glm::vec3 wPos) {
	pos_ = wPos;
	matDirty_ = true;
}

// move the transform by an amount expressed in *WORLD* coordinates
void Transform::moveWorld(glm::vec3 const& delta) {
	pos_ += delta;
	matDirty_ = true;
}

// move the transform by an amount expressed in *LOCAL* coordinates
void Transform::moveLocal(glm::vec3 const& delta) {
	glm::vec3 wDelta = orient_ * delta;
	pos_ += wDelta;
	matDirty_ = true;
}

// rotate the transform by a quaternion expressed in *WORLD* coordinates
void Transform::rotateWorld(glm::quat const& rot) {
	orient_ = glm::normalize(orient_ * rot);
	matDirty_ = true;
}

// rotate the transform by a quaternion expressed in *LOCAL* coordinates
void Transform::rotateLocal(glm::quat const& rot) {
	orient_ = glm::normalize(rot * orient_);
	matDirty_ = true;
}

glm::vec3 Transform::axisX() const {
	glm::vec3 x{1.f, 0.f, 0.f};
	return orient_ * x;
}

glm::vec3 Transform::axisY() const {
	glm::vec3 y{0.f, 1.f, 0.f};
	return orient_ * y;
}

glm::vec3 Transform::axisZ() const {
	glm::vec3 z{0.f, 0.f, 1.f};
	return orient_ * z;
}

const glm::mat4& Transform::glMatrix() const {
	if (matDirty_)
		updateGLMat();
	return glMat_;
}

void Transform::updateGLMat() const {
	glMat_ = glm::toMat4(orient_);
	glMat_[3][0] = pos_.x;
	glMat_[3][1] = pos_.y;
	glMat_[3][2] = pos_.z;
	matDirty_ = false;
}
