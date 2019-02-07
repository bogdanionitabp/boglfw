/*
 * Shape3D.cpp
 *
 *  Created on: Sep 12, 2017
 *      Author: bogdan
 */

#include <boglfw/renderOpenGL/Shape3D.h>
#include <boglfw/renderOpenGL/Renderer.h>
#include <boglfw/renderOpenGL/Viewport.h>
#include <boglfw/renderOpenGL/Camera.h>
#include <boglfw/renderOpenGL/shader.h>
#include <boglfw/math/math3D.h>
#include <boglfw/utils/log.h>

#include <glm/vec4.hpp>
#include <glm/gtx/rotate_vector.hpp>
#include <glm/gtc/type_ptr.hpp>

#define GLEW_NO_GLU
#include <GL/glew.h>
#include <stdexcept>

static Shape3D* instance = nullptr;

void Shape3D::init(Renderer* renderer) {
	instance = new Shape3D(renderer);
}

Shape3D* Shape3D::get() {
	assertDbg(instance && "must be initialized first!");
	return instance;
}

Shape3D::Shape3D(Renderer* renderer) {
	renderer->registerRenderable(this);
	lineShaderProgram_ = Shaders::createProgram("data/shaders/shape3d.vert", "data/shaders/shape3d.frag");
	if (lineShaderProgram_ == 0) {
		throw std::runtime_error("Unable to load shape3D shaders!!");
	}
	indexMatProjView_ = glGetUniformLocation(lineShaderProgram_, "mProjView");

	unsigned indexPos = glGetAttribLocation(lineShaderProgram_, "vPos");
	unsigned indexColor = glGetAttribLocation(lineShaderProgram_, "vColor");

	glGenVertexArrays(1, &VAO_);
	glBindVertexArray(VAO_);
	glGenBuffers(1, &VBO_);
	glBindBuffer(GL_ARRAY_BUFFER, VBO_);
	glGenBuffers(1, &IBO_);
	glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, IBO_);
	glEnableVertexAttribArray(indexPos);
	glEnableVertexAttribArray(indexColor);
	glVertexAttribPointer(indexPos, 3, GL_FLOAT, GL_FALSE, sizeof(s_vertex), (void*)offsetof(s_vertex, pos));
	glVertexAttribPointer(indexColor, 4, GL_FLOAT, GL_FALSE, sizeof(s_vertex), (void*)offsetof(s_vertex, rgba));

	glBindVertexArray(0);
}

Shape3D::~Shape3D() {
	glDeleteProgram(lineShaderProgram_);
	glDeleteVertexArrays(1, &VAO_);
	glDeleteBuffers(1, &VBO_);
	glDeleteBuffers(1, &IBO_);
}

void Shape3D::unload() {
	delete instance;
	instance = nullptr;
}

void Shape3D::render(Viewport* vp, unsigned batchId) {
	assertDbg(batchId < batches_.size());

	unsigned nIndices = batchId == batches_.size() - 1 ? indices_.size() - batches_.back()
		: batches_[batchId+1] - batches_[batchId];
	if (!nIndices)
		return;

	glEnable(GL_BLEND);
	glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA);
	//glBlendEquation(GL_BLEND_EQUATION_ALPHA);

	glUseProgram(lineShaderProgram_);
	glBindVertexArray(VAO_);
	glUniformMatrix4fv(indexMatProjView_, 1, GL_FALSE, glm::value_ptr(vp->camera()->matProjView()));

	glDrawElements(GL_LINES, nIndices, GL_UNSIGNED_INT, (void*)(sizeof(indices_[0]) * batches_[batchId]));

	glDisable(GL_BLEND);
}

void Shape3D::startBatch() {
	batches_.push_back(indices_.size());
}

void Shape3D::setupFrameData() {
	glBindBuffer(GL_ARRAY_BUFFER, VBO_);
	glBufferData(GL_ARRAY_BUFFER, sizeof(s_vertex) * buffer_.size(), &buffer_[0], GL_DYNAMIC_DRAW);
	glBindBuffer(GL_ARRAY_BUFFER, 0);
	glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, IBO_);
	glBufferData(GL_ELEMENT_ARRAY_BUFFER, sizeof(indices_[0]) * indices_.size(), &indices_[0], GL_DYNAMIC_DRAW);
	glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, 0);
}

void Shape3D::purgeRenderQueue() {
	buffer_.clear();
	indices_.clear();
	batches_.clear();
}

void Shape3D::drawLine(glm::vec3 point1, glm::vec3 point2, glm::vec3 rgb) {
	drawLine(point1, point2, glm::vec4(rgb, 1));
}

void Shape3D::drawLine(glm::vec3 point1, glm::vec3 point2, glm::vec4 rgba) {
	glm::vec3* v[] = {&point1, &point2};
	transform(v, 2);
	buffer_.push_back({point1, rgba});
	indices_.push_back(buffer_.size()-1);
	buffer_.push_back({point2, rgba});
	indices_.push_back(buffer_.size()-1);
}

void Shape3D::drawLineList(glm::vec3 verts[], int nVerts, glm::vec3 rgb) {
	drawLineList(verts, nVerts, glm::vec4(rgb, 1));
}

void Shape3D::drawLineList(glm::vec3 verts[], int nVerts, glm::vec4 rgba) {
	transform(verts, nVerts);
	for (int i=0; i<nVerts; i++) {
		buffer_.push_back({verts[i], rgba});
		indices_.push_back(buffer_.size()-1);
	}
}

void Shape3D::drawLineStrip(glm::vec3 verts[], int nVerts, glm::vec3 rgb) {
	drawLineStrip(verts, nVerts, glm::vec4(rgb, 1));
}

void Shape3D::drawLineStrip(glm::vec3 verts[], int nVerts, glm::vec4 rgba) {
	transform(verts, nVerts);
	for (int i=0; i<nVerts; i++) {
		buffer_.push_back({verts[i], rgba});
		indices_.push_back(buffer_.size()-1);
		if (i > 0 && i < nVerts-1)
			indices_.push_back(buffer_.size()-1);
	}
}

void Shape3D::drawPolygon(glm::vec3 verts[], int nVerts, glm::vec3 rgb) {
	drawPolygon(verts, nVerts, glm::vec4(rgb, 1));
}

void Shape3D::drawPolygon(glm::vec3 verts[], int nVerts, glm::vec4 rgba) {
	transform(verts, nVerts);
	for (int i=0; i<nVerts; i++) {
		buffer_.push_back({verts[i], rgba});
		indices_.push_back(buffer_.size()-1);
		if (i > 0) {
			indices_.push_back(buffer_.size()-1);
		}
	}
	indices_.push_back(buffer_.size()-nVerts);
}

void Shape3D::drawRectangleXOY(glm::vec2 pos, glm::vec2 size, glm::vec3 rgb) {
	drawRectangleXOY(pos, size, glm::vec4(rgb, 1));
}

void Shape3D::drawRectangleXOY(glm::vec2 pos, glm::vec2 size, glm::vec4 rgba) {
	glm::vec3 sz = {size, 0};
	glm::vec3 pos3 = {pos, 0};
	glm::vec3 coords[] {
		pos3,
		pos3 + sz.y,
		pos3 + sz,
		pos3 + sz.x
	};
	drawPolygon(coords, 4, rgba);
}

void Shape3D::drawRectangleXOYCentered(glm::vec2 pos, glm::vec2 size, float rotation, glm::vec3 rgb) {
	drawRectangleXOYCentered(pos, size, rotation, glm::vec4(rgb, 1));
}

void Shape3D::drawRectangleXOYCentered(glm::vec2 pos, glm::vec2 size, float rotation, glm::vec4 rgba) {
	float halfW = size.x * 0.5f;
	float halfH = size.y * 0.5f;

	glm::vec3 v[] = {
		glm::vec3(glm::rotate(glm::vec2(-halfW, halfH), rotation) + pos, 0),	// top left
		glm::vec3(glm::rotate(glm::vec2(halfW, halfH), rotation) + pos, 0),		// top right
		glm::vec3(glm::rotate(glm::vec2(halfW, -halfH), rotation) + pos, 0),	// bottom right
		glm::vec3(glm::rotate(glm::vec2(-halfW, -halfH), rotation) + pos, 0),	// bottom left
	};
	transform(v, 4);

	s_vertex sVertex;
	sVertex.rgba = rgba;
	// top left
	sVertex.pos = v[0];
	buffer_.push_back(sVertex);
	indices_.push_back(buffer_.size()-1);
	// top right
	sVertex.pos = v[1];
	buffer_.push_back(sVertex);
	indices_.push_back(buffer_.size()-1);
	indices_.push_back(buffer_.size()-1);
	// bottom right
	sVertex.pos = v[2];
	buffer_.push_back(sVertex);
	indices_.push_back(buffer_.size()-1);
	indices_.push_back(buffer_.size()-1);
	// bottom left
	sVertex.pos = v[3];
	buffer_.push_back(sVertex);
	indices_.push_back(buffer_.size()-1);
	indices_.push_back(buffer_.size()-1);
	// top left again
	indices_.push_back(buffer_.size()-4);
}

void Shape3D::drawCircleXOY(glm::vec2 pos, float radius, int nSides, glm::vec3 rgb) {
	drawCircleXOY(pos, radius, nSides, glm::vec4(rgb, 1));
}

void Shape3D::drawCircleXOY(glm::vec2 pos, float radius, int nSides, glm::vec4 rgba) {
	// make a polygon out of the circle
	float phiStep = 2 * PI * 1.f / nSides;
	glm::vec3 *v = new glm::vec3[nSides];
	float phi = 0;
	for (int i=0; i<nSides; i++) {
		v[i] = glm::vec3(pos, 0) + glm::vec3{cosf(phi) * radius, sinf(phi) * radius, 0};
		phi += phiStep;
	}
	drawPolygon(v, nSides, rgba);
	delete [] v;
}

void Shape3D::drawAABB(AABB const& aabb, glm::vec3 rgb) {
	drawAABB(aabb, glm::vec4(rgb, 1.f));
}

void Shape3D::drawAABB(AABB const& aabb, glm::vec4 rgba) {
	glm::vec3 verts[8] {
		aabb.vMin,								 // bottom left back
		{aabb.vMin.x, aabb.vMax.y, aabb.vMin.z}, // top left back
		{aabb.vMin.x, aabb.vMax.y, aabb.vMax.z}, // top left front
		{aabb.vMin.x, aabb.vMin.y, aabb.vMax.z}, // bottom left front
		{aabb.vMax.x, aabb.vMin.y, aabb.vMin.z}, // bottom right back
		{aabb.vMax.x, aabb.vMax.y, aabb.vMin.z}, // top right back
		aabb.vMax, 								 // top right front
		{aabb.vMax.x, aabb.vMin.y, aabb.vMax.z}, // bottom right front
	};
	drawLine(verts[0], verts[1], rgba);
	drawLine(verts[1], verts[2], rgba);
	drawLine(verts[2], verts[3], rgba);
	drawLine(verts[3], verts[0], rgba);
	drawLine(verts[4], verts[5], rgba);
	drawLine(verts[5], verts[6], rgba);
	drawLine(verts[6], verts[7], rgba);
	drawLine(verts[7], verts[4], rgba);
	drawLine(verts[0], verts[4], rgba);
	drawLine(verts[1], verts[5], rgba);
	drawLine(verts[2], verts[6], rgba);
	drawLine(verts[3], verts[7], rgba);
}

void Shape3D::setTransform(glm::mat4 mat) {
	transform_ = mat;
	transformActive_ = true;
}

void Shape3D::resetTransform() {
	transformActive_ = false;
}

void Shape3D::transform(glm::vec3* v[], int n) {
	if (!transformActive_)
		return;
	for (int i=0; i<n; i++)
		*v[i] = vec4xyz(transform_ * glm::vec4(*v[i], 1));
}

void Shape3D::transform(glm::vec3 v[], int n) {
	if (!transformActive_)
		return;
	for (int i=0; i<n; i++)
		v[i] = vec4xyz(transform_ * glm::vec4(v[i], 1));
}
