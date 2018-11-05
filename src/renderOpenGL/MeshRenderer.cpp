/*
 * MeshRenderer.cpp
 *
 *  Created on: Apr 25, 2017
 *      Author: bog
 */

#include <boglfw/renderOpenGL/MeshRenderer.h>
#include <boglfw/renderOpenGL/Renderer.h>
#include <boglfw/renderOpenGL/Viewport.h>
#include <boglfw/renderOpenGL/Mesh.h>
#include <boglfw/renderOpenGL/shader.hpp>
#include <boglfw/renderOpenGL/Camera.h>

#include <boglfw/utils/assert.h>

#include <glm/gtc/type_ptr.hpp>

#include <GL/glew.h>
#include <GL/gl.h>


static MeshRenderer* instance = nullptr;

void MeshRenderer::init(Renderer* renderer) {
	instance = new MeshRenderer(renderer);
}

MeshRenderer* MeshRenderer::get() {
	assertDbg(instance && "must be initialized first!");
	return instance;
}

void MeshRenderer::unload() {
	delete instance;
	instance = nullptr;
}

MeshRenderer::MeshRenderer(Renderer* renderer) {
	renderer->registerRenderable(this);
	meshShaderProgram_ = Shaders::createProgram("data/shaders/mesh.vert", "data/shaders/mesh-texture.frag");
	if (meshShaderProgram_ == 0) {
		throw std::runtime_error("Unable to load mesh shaders!!");
	}
	indexPos_ = glGetAttribLocation(meshShaderProgram_, "vPos");
	indexNorm_ = glGetAttribLocation(meshShaderProgram_, "vNormal");
	indexUV1_ = glGetAttribLocation(meshShaderProgram_, "vUV1");
	indexColor_ = glGetAttribLocation(meshShaderProgram_, "vColor");
	indexMatPVW_ = glGetUniformLocation(meshShaderProgram_, "mPVW");
}

MeshRenderer::~MeshRenderer() {
	glDeleteProgram(meshShaderProgram_);
}

void MeshRenderer::renderMesh(Mesh& mesh, glm::mat4 worldTransform) {
	renderQueue_.push_back(meshRenderData(&mesh, worldTransform));
}

void MeshRenderer::render(Viewport* vp, unsigned batchId) {
	assertDbg(batchId < batches_.size());
	glUseProgram(meshShaderProgram_);
	glEnableVertexAttribArray(indexPos_);
	glEnableVertexAttribArray(indexNorm_);
	glEnableVertexAttribArray(indexUV1_);
	glEnableVertexAttribArray(indexColor_);

	auto matPV = vp->camera()->matProjView();

	unsigned nMeshes = batchId == batches_.size() - 1 ? renderQueue_.size() - batches_.back()
		: batches_[batchId+1] - batches_[batchId];

	for (unsigned i=0; i<nMeshes; i++) {
		auto &m = renderQueue_[batches_[batchId] + i];
		if (m.pMesh_->isDirty()) {
			m.pMesh_->commitChanges();
		}
		auto matPVW = matPV * m.wldTransform_;
		glUniformMatrix4fv(indexMatPVW_, 1, GL_FALSE, glm::value_ptr(matPVW));

		glBindBuffer(GL_ARRAY_BUFFER, m.pMesh_->getVertexBuffer());
		glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, m.pMesh_->getIndexBuffer());
		glVertexAttribPointer(indexPos_, 3, GL_FLOAT, GL_FALSE, sizeof(Mesh::s_Vertex), (void*)offsetof(Mesh::s_Vertex, position));
		glVertexAttribPointer(indexNorm_, 3, GL_FLOAT, GL_FALSE, sizeof(Mesh::s_Vertex), (void*)offsetof(Mesh::s_Vertex, normal));
		glVertexAttribPointer(indexUV1_, 2, GL_FLOAT, GL_FALSE, sizeof(Mesh::s_Vertex), (void*)offsetof(Mesh::s_Vertex, UV1));
		glVertexAttribPointer(indexColor_, 4, GL_FLOAT, GL_FALSE, sizeof(Mesh::s_Vertex), (void*)offsetof(Mesh::s_Vertex, color));

		glDrawElements(GL_TRIANGLES, m.pMesh_->getIndexCount(), GL_UNSIGNED_SHORT, 0);
	}
	glBindBuffer(GL_ARRAY_BUFFER, 0);
	glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, 0);
}

void MeshRenderer::startBatch() {
	batches_.push_back(renderQueue_.size());
}

void MeshRenderer::purgeRenderQueue() {
	renderQueue_.clear();
	batches_.clear();
}
